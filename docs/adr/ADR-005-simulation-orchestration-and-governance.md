# ADR-005: Simulation Orchestration, Extended Rules & Anomaly Governance

## Status
**Approved** — 2026-11-19

## Context

ADR-004 established the three architectural pillars (O(V+E) video-centric loop, `TemporalRegistry`, stats↔events coupling) and recorded the R13/R4/R5 revisions. Since then the simulation engine has grown a full orchestration layer, five additional domain rules, a global anomaly coordinator, and a deterministic-replay discipline. This ADR captures those evolutions so the architecture stays documented in one place.

## Decision

We adopt four interlocking extensions on top of ADR-004:

### 1. 8-Phase Orchestration with 4 Progressive Quality Gates

The single-shot validation model is replaced by a phased pipeline where each phase ends with a fail-fast gate:

| Phase | Name | Gate | Invariant Checker |
|-------|------|------|-------------------|
| 1 | Initialize & Clock | — | `BusinessClock`, `RandomProvider(seed=42)`, `MatchRegistry` |
| 2 | PostgreSQL Domain | **Gate I** | `assert_subscriptions_non_overlapping()` |
| 3 | MongoDB Domain | — | `MatchRegistry` registration, `create_match()` |
| 4 | Temporal Isolation | **Gate II** | `assert_video_availability_windows_reachable()` |
| 5 | Session Topology | — | `SessionFactory(registry)` with engagement tiers |
| 6 | Kafka Event Pipeline | **Gate III** | Multi-entity structural verification |
| 7 | Anomaly Injection | **Gate IV** | Volume-weighted anomaly rate within [1%, 5%] |
| 8 | Bulk Loading Pipeline | — | Parallel commit to Postgres, Mongo, Kafka |

Gates I–IV are **active assertions** in `run_simulation.py` (no longer behind `# noqa`); a failure stops the pipeline before the next phase begins.

### 2. Extended Domain Rules (R6–R19)

| Rule | Summary | Enforcement |
|------|---------|-------------|
| **R6** | Billing/contract temporal validation | `registry/billing_eligibility.py` |
| **R7** | `TemporalRegistry` eligibility gate — `paid_subscription_ids` + `active_user_ids` filters applied at **load time** | `temporal_registry.py:load_from_memory()` |
| **R8** | User lifecycle evaluation (suspend/delete) feeds `active_user_ids` | `postgres/user_lifecycle.py` |
| **R9** | Subscription renewal logic with grace period | `postgres/subscription_factory.py` |
| **R10** | Payment retry/renewal with `attempt_no` (1–3) | `postgres/payment_factory.py` |
| **R11** | Engagement-tiered sessions: HIGH/MEDIUM/LOW with distinct `sessions/yr`, `events/session`, `like_ratio` | `registry/session_factory.py` |
| **R12** | Like-ratio reconciliation per tier, preserving `stats.likes == like_events` | `reconcile_likes_with_stats()` |
| **R13** | Behaviour-first budget (already in ADR-004) | `config.py` + `run_simulation.py` |
| **R14** | Popularity normalizer so `Σ latent_popularity = derived_budget` | `video_simulation.py:_compute_popularity_normalizer()` |
| **R15** | Single `PLAN_PRICING` table — all amounts via `PaymentFactory.get_amount()` | `postgres/payment_factory.py` |
| **R16** | `duration_sec` for live/replay = match duration; highlight = clip | `video_simulation.py` |
| **R17** | `status` derived from window vs `created_at` vs horizon | `video_simulation.py:_derive_status()` |
| **R18** | `watch_time_sec` bounds per content type (`WATCH_TIME_BOUNDS`), capped by duration | `video_simulation.py` + `kafka/interaction_factory.py` |
| **R19** | Single `RandomProvider(seed=42)` shared everywhere → bit-for-bit deterministic replay | `random_provider.py` injection pattern |
### 3. GlobalAnomalyCoordinator — Flat 3.5% Target with LLN Convergence

Instead of partitioning an anomaly budget procedurally, each data stream receives an **independent flat target of 3.5%**. Volume-weighted blending emerges naturally via the Law of Large Numbers and inherent Poisson noise in the generation pipeline:

```python
# anomaly_coordinator.py
TARGET_BLENDED_RATE = 0.035
MIN_BLENDED_RATE = 0.01
MAX_BLENDED_RATE = 0.05
```

- High-volume streams (Kafka events, payments) stabilise near 3.5% with minimal variance.
- Low-volume streams (videos) exhibit higher sampling variance — accepted; the **blended** rate across the full dataset stays inside [1%, 5%].
- `calculate_rates()` assigns the flat rate to every registered stream; `get_injector_kwargs()` surfaces the rates for the three dirty injectors.
- Deep-copy micro-mutation preserves pristine masters for downstream telemetry.

### 4. Shared `RandomProvider` Dependency Injection (R19)

A single `RandomProvider(seed=42)` instance is constructed in `run_simulation.py:main()` and **injected** into every component that needs randomness:

- `MatchRegistry` (match registration order)
- `VideoSimulation` (latent popularity, content-type weights)
- `SessionFactory` (tier assignment, session counts)
- `InteractionFactory` (event timestamps, watch times)
- `GlobalAnomalyCoordinator` → all three dirty injectors
- `BusinessClock` forks for per-entity timelines

This guarantees **bit-for-bit deterministic replay** — identical seed produces identical full datasets. Verified by `tests/simulator/test_deterministic.py`.

### 5. Phase-Marker Convention

Every simulation module uses `# -- Phase X: name --` comment markers that map 1:1 to the 8-phase orchestration:

| Phase | Marker | Files |
|-------|--------|-------|
| 1 | `# -- Phase 1: Initialize & Clock --` | `clock.py`, `random_provider.py`, `id_generator.py`, `match_registry.py` |
| 2 | `# -- Phase 2: PostgreSQL Domain --` | `postgres/*.py` |
| 3 | `# -- Phase 3: MongoDB Domain --` | `mongo/video_simulation.py` |
| 4 | `# -- Phase 4: Temporal Isolation --` | `temporal_registry.py`, `registry/billing_eligibility.py` |
| 5 | `# -- Phase 5: Session Topology --` | `registry/session_factory.py` |
| 6 | `# -- Phase 6: Kafka Event Pipeline --` | `kafka/*.py` |
| 7 | `# -- Phase 7: Anomaly Injection --` | `anomaly_coordinator.py`, `dirty_data/*.py` |
| 8 | `# -- Phase 8: Bulk Loading Pipeline --` | `loading/*.py`, `run_simulation.py` |

The marker table in `README.md` is the authoritative cross-reference; grep for `# -- Phase` to navigate the codebase.

## Consequences

### Positive
- **Fail-fast**: invalid data never reaches the next phase; bugs surface at the gate that detects them.
- **Deterministic replay**: single seed → identical datasets; essential for regression testing and incident reproduction.
- **Anomaly honesty**: flat 3.5% per stream + LLN blending means no hidden budget shuffling; the blended rate is a measured outcome, not a configured target.
- **Rule traceability**: R6–R19 each have a named enforcer; adding a rule means adding a checker, not a comment.

### Negative / Trade-offs
| Trade-off | Mitigation |
|-----------|------------|
| **More phases = more files** | Phase markers keep navigation linear; `run_simulation.py` remains the single orchestration script. |
| **LLN blending means low-volume streams can deviate** | Bounds [1%, 5%] are hard assertions in Gate IV; deviations are visible, not absorbed. |
| **Deepcopy overhead for dirty injection** | ~2 s for 10 K users; acceptable for batch pipeline. |
| **R7 eligibility gate adds load-time filtering** | Gate is optional (`None` = no gating); downstream ETL/tests unaffected. |

### Risks Accepted
- **Phase coupling**: Gate III depends on Gate II's registry being fully loaded — enforced by orchestration order.
- **Anomaly injector coupling**: injectors share the coordinator's `RandomProvider`; changing injection logic requires coordinator update.
- **No streaming path yet** — Phase 5 in ADR-004 migration table remains planned.

## Implementation References
- `scripts/simulation/run_simulation.py` — 8-phase orchestration, Gates I–IV, R13 accounting
- `scripts/simulation/invariants.py` — canonical checkers for Gates I, II, III, IV
- `scripts/simulation/temporal_registry.py` — R7 eligibility gate
- `scripts/simulation/registry/session_factory.py` — R11 engagement tiers, R12 like reconciliation
- `scripts/simulation/anomaly_coordinator.py` — flat 3.5% target, LLN blending, bounds
- `scripts/simulation/random_provider.py` — single RNG, injected everywhere (R19)
- `scripts/simulation/mongo/video_simulation.py` — R14, R16, R17, R18
- `scripts/simulation/dirty_data/*.py` — deepcopy micro-mutation at coordinator rates
- `config.py` — volumetric knobs, `SIM_MIN_STATS_VIEWS/LIKES`, `PLAN_PRICING`

## Migration Path
| Phase | Status | Description |
|-------|--------|-------------|
| 1–4 | ✅ Done | ADR-004 pillars + R13/R4/R5 |
| 5 | ✅ Done | 8-phase orchestration, Gates I–IV, phase markers |
| 6 | ✅ Done | R6–R12, R15–R19 rules implemented |
| 7 | ✅ Done | `GlobalAnomalyCoordinator`, deterministic replay |
| 8 | 📋 Planned | Streaming producer, CDC integration, Power BI semantic model |

---

*This ADR records the orchestration, governance, and deterministic-replay layer that sits on top of the ADR-004 pillars — completing the architectural picture of the simulation engine.*