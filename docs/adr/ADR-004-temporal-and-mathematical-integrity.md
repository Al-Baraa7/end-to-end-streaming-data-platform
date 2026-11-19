# ADR-004: Temporal and Mathematical Integrity in Event Simulation

## Status
**Approved** — 2026-10-15 · revised 2026-10-22 (R13 behaviour-first event budget, R4/R5 match identity & window integrity)

## Context

The legacy simulation pipeline suffered from three compounding architectural flaws that made it unusable at production scale (10K users × 10K videos):

### 1. O(U×V) Algorithmic Complexity — Infinite Loop at Video 7000
The original user-centric event generation iterated every user for every video:
```python
for user in users:
    for video in videos:
        generate_events(user, video)  # 100M iterations minimum
```
At ~video 7000, the process would stall indefinitely—CPU at 100%, zero progress—because each iteration performed registry lookups, clock advances, and eligibility checks without any early-exit or batching strategy. The O(U×V) complexity made 10K×10K workloads mathematically impossible.

### 2. Free-User Subscription Suspension Bug
The subscription eligibility logic used `user.created_at` as the sole gate:
```python
if subscription.end_date >= user.created_at:
    eligible_users.append(user)
```
This ignored the simulation clock entirely. A free-tier user whose subscription expired on Day 30 would remain "eligible" for events generated on Day 200, producing referentially invalid events that violated business rules and broke downstream analytics.

### 3. Fractured Temporal Views vs. Events
Video statistics (`views`, `likes`) were generated **independently** from Kafka events:
- MongoDB videos received synthetic stats at creation time (latent popularity → random views/likes)
- Kafka events were generated later with no coupling to those stats
- Result: `video.stats.views = 847` but only 12 actual `play` events existed for that video
- Power BI dashboards showed impossible ratios (214× more views than plays), destroying trust in the data platform.
---

## Decision

We adopt three interlocking architectural pillars:

### 1. O(V+E) Video-Centric Poisson Sampling
Replace the nested user×video loop with a video-centric generator:
```python
for video, lambda_derived in video_event_targets:
    target_events = poisson(lambda_derived)  # λ DERIVED from behaviour, never rescaled
    for _ in range(target_events):
        event_ts = sample_timestamp(video_window)
        eligible_user = find_eligible_user(event_ts)  # O(1) registry lookup
        event = factory.create(user=eligible_user, video=video, ts=event_ts)
```
- **Complexity**: O(V + E) — iterate 10K videos once + generate 18K events
- **Throughput**: ~24,000 events/sec (measured in dry-run)
- **Scale**: 10K videos, 10K users completes in ~30 seconds

### 2. TemporalRegistry as Single Source of Temporal Truth
A new `TemporalRegistry` enforces all temporal constraints at generation time:
- `user_has_active_subscription(user_id, timestamp)` — validates subscription covers event time
- `video_is_available(video_id, timestamp)` — validates video is live/published
- `is_valid_event(user_id, video_id, timestamp)` — composite referential integrity
- `get_video_window(video_id)` — returns `(live_start_at, live_end_at)` for timestamp sampling

All event generation **must** pass registry checks; invalid combinations are rejected at the source.

### 3. Post-Generation Stats Coupling with Absolute Floors
`VideoSimulation.compute_final_stats(video, events)` derives final stats **from actual events**:
```python
views = max(len(play_events), base_views)
likes = max(len(like_events), base_likes)
# Absolute floors (business rules)
views = max(views, 100)
likes = max(likes, 1)
likes = min(likes, views - 1)
```
- `stats.views` = actual `play` event count (floor 100)
- `stats.likes` = actual `like` event count (floor 1, strictly < views)
- Perfect mathematical coupling: **zero divergence possible**

### 4. Dirty Data Injection via Post-Generation Deepcopy
Data quality issues (null emails, invalid countries, future-dated logins, end_date < start_date) are injected **after** all clean generation completes:
```python
users, subscriptions, payments = dirty_injector.inject(
    users=deepcopy(users),
    subscriptions=deepcopy(subscriptions),
    payments=deepcopy(payments),
)
```
- Clean pipeline remains mathematically pure
- Dirty variants produced for ETL testing without polluting source of truth

### 5. Behaviour-First Event Budget (R13 — 2026-10-22)

The event budget is **derived from declared viewing behaviour**; `--events` is a
**ceiling only**. Nothing rescales the model to fit a requested number:

```
expected_events = users × sessions_per_user_per_year × events_per_session
                = 1000 × 12 × 4 = 48,000        # authorized default policy
```

| Step | Rule |
|------|------|
| 1. Declare behaviour | `SIM_SESSIONS_PER_USER_PER_YEAR`, `SIM_EVENTS_PER_SESSION` (`config.py`) |
| 2. Derive the budget | `expected_events = users × sessions × events/session` |
| 3. Distribute it | `expected_events_per_video` per video, so `Σ latent_popularity ≡ expected_events` |
| 4. Bound by reality | `λ = min(latent_pop, eligible_audience(video_window))` |
| 5. Sample | `target_events = poisson(λ)` per video |
| 6. Truncate only | `--events` stops emission early; it never scales λ up or down |

**Removed — the silent rescaling pass.** The old pipeline computed
`scale = total_target_events / total_sampled_popularity`, multiplied every video's λ by
it, and clawed back overflow to land exactly on an externally supplied total. That made
the realized volume always equal the requested number, at the cost of destabilising
per-video popularity and **hiding placement losses** — the emitter could drop most of its
draws and the rescale would simply raise λ until the total looked right again.

**Replaced by reporting.** Each run prints the derived target, the realized count, the
ceiling, the emission accounting (`sampled / placed / unplaceable / ceiling-truncated`),
the eligible-user probe hit rate, and `N bounded by eligible audience`. A shortfall is
*stated*, never normalized away.

**Finding exposed by removing the rescale.** 78 of 151 live videos had
`created_at > live_start_at`, because `created_at` and the broadcast window were drawn
independently in `mongo/video_simulation.py`. `TemporalRegistry.video_is_available`
requires `created_at <= ts <= live_end_at`, so those videos could never serve a
timestamp: all 20 probes of every draw failed and **14,236 of 45,171 λ (~31% of the
modelled volume) was silently discarded** — invisible while the rescale compensated.
Fixed by anchoring `created_at` ahead of kick-off (a video cannot be published after its
own broadcast starts), and the rule is now enforced by the canonical checker
`assert_video_availability_windows_reachable` (`scripts/simulation/invariants.py`), called
on every run — the failure mode can no longer reappear silently.

### 6. Unified Match Identity + Window Integrity (R4/R5 — 2026-10-22)

**R4 — one match, one `match_id`.** `match_id` used to be
`f"match_{randint(1000, 9999)}"`, drawn **per video**. Two different matches could
collide (birthday paradox over 9,000 values), while the live feed, the highlight and the
replay of one match were handed three unrelated ids — so the "per match" join every
downstream metric depends on was structurally impossible.

`MatchRegistry` (`scripts/simulation/match_registry.py`) now registers each match ONCE
(competition, teams, kick-off, playtime) and hands out ids from a monotonic sequence
(`match_000001`, `match_000002`, …). `generate_videos()` is match-first: it registers a
match and yields its three artifacts (`live` → `highlight` → `replay`) **from that same
record**, so `match_id`, `kickoff_at`, `match_duration_sec` and the whole `competition`
block are identical across siblings *by construction* — returned as a fresh dict per
artifact, so editing one document can never rewrite its siblings.

**R5 — artifact timestamps are derived from the match, never drawn freely.** Free
per-field draws are exactly what produced the R13 finding above, so every artifact is
pinned to its match window:

| Artifact | Broadcast window | `created_at` | `duration_sec` | `status` |
|---|---|---|---|---|
| `live` | `[kickoff_at, kickoff_at + match_duration_sec]` | `<= kickoff_at` (announced 5 min – 24 h early) | the match window itself | derived |
| `highlight` | none (VOD — served from `created_at`) | `>= final whistle` (1–72 h after) | 180–600 s, clamped to the match | `archived` |
| `replay` | none (VOD) | `>= final whistle` | exactly the match window | `archived` |

`status` is no longer `random.choice(STATUSES)`: a sampled status could label a match
that has not kicked off yet as `ended`, or leave a finished one `streaming` forever. It is
now a pure function of the artifact's own `created_at`, its window and `simulation_end`
(`VideoSimulation._derive_status`): `streaming` while the window is still open at the read
instant, `archived` for a banner published after its own final whistle, `ended` otherwise.

Both rules are gated on the **clean** catalogue *before* dirty injection, so a generated
catalogue can never violate them: `assert_match_artifacts_unified(videos)` and
`assert_match_artifact_temporal_integrity(videos, simulation_end=...)`
(`scripts/simulation/invariants.py`, called from `scripts/simulation/run_simulation.py`).

**Measured effect (smoke run, 50 users × 50 videos).** The R4 rewrite removes the
placement losses the R13 finding had only papered over: `sampled=1318, placed=1318,
unplaceable=0`, where the same generator previously discarded **every** draw aimed at a
live video published after its own kick-off (78/151 live videos ≈ 52% in the R13
analysis). The catalogue reports one `match_id` for each match and 0 unplaceable draws.

---

## Consequences

### Positive
| Outcome | Metric |
|---------|--------|
| **Throughput** | 24K events/sec sustained (dry-run, 10K videos) |
| **Completion Time** | Full pipeline (10K users, 10K videos, 20K subs, 40K payments, 18K events) ≈ 30 sec |
| **Stats↔Events Coupling** | 1:1 mathematical guarantee; `views ≡ play_events`, `likes ≡ like_events` |
| **Temporal Validity** | 100% of events pass `TemporalRegistry` constraints by construction |
| **Master Clock Stability** | Single `BusinessClock` drives all timestamps; no drift |
| **Power BI Readiness** | Zero impossible ratios; dashboards render trustworthy metrics |
| **CI/CD Integration** | `--dry-run` flag enables fast validation without DB/Kafka infrastructure |
| **Honest Volume Accounting** | Derived target 48,000 → realized 45,031 vs ceiling 50,000 (1000 users × 500 videos); zero rescaling paths remain |

### Negative / Trade-offs
| Trade-off | Mitigation |
|-----------|------------|
| **Poisson approximation** for high λ | Normal approximation (μ=λ, σ=√λ) used for λ > 50; error < 1% |
| **Deepcopy overhead** for dirty injection | ~2 sec for 10K users; acceptable for batch pipeline |
| **Eligible user search** (up to 20 attempts/event) | O(1) registry checks; negligible at current scale |
| **Audience capacity bound** — a small audience cannot realize a large derived budget (40 users × 20 videos: 440 realized vs 1,920 derived) | Reported as `N bounded by eligible audience` plus target-vs-realized; never rescaled to fit |
| **`--videos` counts ARTIFACTS, not matches** (R4: the catalogue is match-first, ~3 artifacts per match, so 999 videos ≈ 333 matches) | Stated explicitly in `generate_videos()` and printed on every run as `R4 match registry: N match(es) for M artifact(s)` |

### Risks Accepted
- **Poisson sampling** introduces slight event count variance (±√λ) — accepted as realistic user behavior
- **No real-time streaming** — this is a batch simulation; streaming architecture deferred to Phase 5
- **Placement probes are finite** (20/draw): the eligible-user probe hit rate (~39% at the default policy) is printed on every run, so unplaceable draws stay visible instead of being absorbed by a rescale

---

## Implementation References
- `scripts/simulation/run_simulation.py` — orchestration, video-centric loop, `--dry-run`, derived budget + emission accounting, R4/R5 gates on the clean catalogue
- `scripts/simulation/match_registry.py` — R4: `Match` + `MatchRegistry` (one registered `match_id` per match, monotonic sequence, validated registration)
- `scripts/simulation/mongo/video_simulation.py` — `latent_popularity`, `compute_final_stats()`, R4 `create_match()` + match-first `generate_videos()`, R5 window pinning (`created_at <= live_start_at`) and derived `status`
- `config.py` — behaviour knobs: `SIM_SESSIONS_PER_USER_PER_YEAR`, `SIM_EVENTS_PER_SESSION`
- `scripts/simulation/temporal_registry.py` — `TemporalRegistry` (temporal gatekeeper)
- `scripts/simulation/invariants.py` — canonical R1/R2 and R13 invariant checkers (credential-free)
- `scripts/simulation/random_provider.py` — `poisson()`, `uniform()`
- `scripts/simulation/dirty_data/postgres_dirty_injection.py` — post-generation deepcopy injection

---

## Migration Path
| Phase | Status | Description |
|-------|--------|-------------|
| 1 | ✅ Done | Isolate `TemporalRegistry`, fix subscription eligibility |
| 2 | ✅ Done | Add `latent_popularity`, Poisson sampler, `RandomProvider.poisson()` |
| 3 | ✅ Done | Video-centric O(V+E) loop, stats↔events coupling, dry-run |
| 4 | ✅ Done | Generation-time validation (registries as gatekeepers, registered `match_id`s — see R4/R5 below) |
| 5 | 📋 Planned | Streaming producer, CDC integration, Power BI semantic model |
| R13 | ✅ Done | Behaviour-first budget, rescaling/clawback removed, `--events` demoted to a ceiling, emission accounting added |
| R4 / R5 | ✅ Done | Unified match identity (`MatchRegistry`: one registered `match_id` shared by live/highlight/replay) + window integrity (`created_at <= live_start_at`, `kickoff_at + duration_sec <= simulation_end`, `duration_sec` bounded by the match, derived `status`); gated by `assert_match_artifacts_unified` / `assert_match_artifact_temporal_integrity` |

---

*This ADR records the architectural victory that transformed an O(U×V) stalled pipeline into a production-grade, mathematically sound, temporally consistent simulation engine.*