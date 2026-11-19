# Simulation Engine — Unified Documentation

**Purpose**: Generate a deterministic, referentially-consistent dataset for a football-video streaming platform across PostgreSQL (users/subscriptions/payments), MongoDB (videos), and Kafka (interaction events).

## Execution Topology & Progressive Quality Gates

The simulation architecture rejects the naive approach of single-phase end-of-pipeline validation. Instead, it enforces a **Fail-Fast Methodology** using four explicit **Progressive Quality Gates (Invariants)** embedded directly within the orchestration pipeline to guarantee referential and temporal consistency before state transitions occur.

### Pipeline Phases
1. **Initialize** — `BusinessClock` (UTC, seed=42), `RandomProvider` (single instance), `MatchRegistry` (monotonic `match_000001...`)
2. **PostgreSQL Execution** — Generate core entities (Users, Subscriptions, Payments).
   * 🛑 **Quality Gate I (Post-Postgres)**: Enforces `assert_subscriptions_non_overlapping()` to guarantee strict sequential subscription boundaries.
3. **MongoDB Content Setup** — Build video metadata artifacts grouped by match blocks.
4. **Temporal State Building** — Build the global composite metadata and availability map.
   * 🛑 **Quality Gate II (Mid-Pipeline)**: Enforces `assert_video_availability_windows_reachable()` to isolate temporal anomalies before runtime.
5. **Session Engineering** — Generate engagement-tiered viewing sessions.
6. **Event Pipeline Flattening** — Enforce Kafka transaction bounds and apply like-ratio couplings.
   * 🛑 **Quality Gate III (Pre-Dirty Master Check)**: Multi-entity verification verifying that the pristine dataset conforms 100% to all core structural business invariants.
7. **Dirty Data Injection** — Micro-mutate copies of pristine records via the coordinator.
   * 🛑 **Quality Gate IV (Post-Dirty Integrity)**: Confirms that structural anomalies fall precisely within configured analytical limits.
8. **Load & Produce** — Final parallel commit via PostgreSQL bulk load, MongoDB batch insert, and Kafka stream production.

---

## Glossary: Logical Name → Real Meaning

| Logical Name | Real Meaning |
|--------------|--------------|
| `SIM_KAFKA_EVENT_CEILING` | Safety ceiling on total Kafka events (50,000 default). The **derived target** is `users × sessions/user/yr × events/session` (default 48,000). Realized events ≤ ceiling; landing under target is legitimate Poisson noise, never rescaled. |
| `KAFKA_EVENT_CEILING` | Local alias for the ceiling in `run_simulation.py`. |
| `calculate_rates()` | Assigns a flat target anomaly rate of 3.5% to each individual data stream independently. Global volume-weighted blending emerges naturally through statistical convergence driven by the Law of Large Numbers. |
| `reconcile_likes_with_stats()` | Tier-aware like reconciliation. Adjusts likes within each engagement tier: HIGH 2-3%, MEDIUM 1-2%, LOW 0.5-1%, while maintaining `stats.likes == like_events`. |
| `SessionFactory(registry)` | Receives the live operational TemporalRegistry instance directly to drive sequential session generation and enforce strict temporal boundaries. |

## Anomaly Rate

The system targets a volume-weighted blended anomaly rate of 3.5% across the entire simulation ecosystem. 

Rather than artificially partitioning an anomaly budget via procedural code—which violates entity simulation independence—each data stream is independently assigned a flat target rate of 3.5%. Due to the **Law of Large Numbers** and inherent **Poisson Noise** in the generation pipeline:
- **High-Volume Streams** (e.g., Kafka events, payments) naturally stabilize with minimal variance, landing precisely near the 3.5% target.
- **Low-Volume Streams** (e.g., videos) exhibit healthy statistical variance (floating naturally within a strict 1%–5% analytical bound).

The mathematical weighted average across all generated records converges perfectly to the 3.5% blended target. This approach enforces pure independent entity lifecycles while producing realistic, volume-weighted statistical behavior.

## Like Ratio

Tier-stratified like ratios enforced per engagement tier:
- HIGH engagement: 2-3%
- MEDIUM engagement: 1-2%
- LOW engagement: 0.5-1%
Reconciliation is performed within each tier's session pool to preserve engagement semantics.

---

## All Invariants Explained
### R1 — Horizon Containment (Subscription Truncation)
**Rule**: No subscription window may exceed `SIMULATION_END`. Windows are clamped at the horizon.
**Enforcement**: `SubscriptionFactory._compute_is_active()` uses `simulation_end` as reference; `SubscriptionEntity.is_truncated` telemetry flag.
**Why**: Guarantees child clocks (`BusinessClock.fork()`) never advance past horizon during payment retry chains.

### R2 — Subscription Non-Overlapping
**Rule**: Per-user subscriptions are strictly sequential: `start_{i+1} > end_i`. Gaps allowed, overlaps forbidden.
**Enforcement**: `assert_subscriptions_non_overlapping()` in `invariants.py` — called as quality gate in `main()`.
**Why**: Prevents double-billing and ambiguous active-window resolution for event sampling.

### R4 — Match Concept (Single Source of Truth)
**Rule**: A match is registered **once** (`MatchRegistry.monotonic sequence`). Its live broadcast, highlight, and replay are three artifacts **of** that match — they share `match_id`, `kickoff_at`, `match_duration_sec`, `competition` block **verbatim**.
**Enforcement**: `MatchRegistry` hands out zero-padded `match_000001...` IDs. `VideoSimulation.generate_videos()` emits artifacts grouped by match. `assert_match_artifacts_unified()` validates.
### R5 — Video Temporal Integrity
**Rule**: Per artifact type:
- **Live**: `created_at <= live_start_at (= kickoff_at)`, `live_end_at = kickoff_at + match_duration_sec`, `duration_sec = match_duration_sec`, window within horizon.
- **VOD (highlight/replay)**: `created_at >= match_end_at` (cut after final whistle), no broadcast window, replay `duration_sec = match_duration_sec`.
- **Status**: Derived from window vs `created_at` vs horizon — never a free 3-way choice.
**Enforcement**: `assert_match_artifact_temporal_integrity()` in `invariants.py` — quality gate in `main()`.

### R6 — Payment Activation Gate
**Rule**: A subscription is **active** iff:
```
has_success_payment AND (start_date <= horizon <= end_date)
```
Free plans are **exempt** (never billed, so `has_success_payment` is vacuously true).
**Enforcement**: `BillingEligibility.apply_activation_gate()` in `billing_eligibility.py` — pure set arithmetic O(S+P) via payment index.

### R7 — Eligibility Gate for Event Stream
**Rule**: The Kafka event stream may **only** sample `(user_id, video_id, timestamp)` where:
- User's account status is `active` (R8 verdict at horizon)
- User owns at least one **paid-for** subscription window covering `timestamp`
- Video is available at `timestamp` (TemporalRegistry composite check)
**Enforcement**: `TemporalRegistry.load_from_memory(paid_subscription_ids=..., active_user_ids=...)` — R7-gated load excludes unpaid windows. `is_valid_event()` is the single composite gate.

### R8 — Account Status at Horizon
**Rule**: Persisted `account_status` describes the account at `SIMULATION_END`, not at generation time.
**Enforcement**: `UserSimulation.evaluate_lifecycle(now=SIMULATION_END)` via `UserLifecycle.evaluate_until_stable()` — replays transitions (active→suspended→deleted) until stable. Free users short-circuit to `active`.

### R11 — Like Ratio Coupling
**Rule**: `stats.likes == like_events` AND `stats.views == play_events` exactly (floor 0). Likes ≤ Views - 1.
**Enforcement**: `VideoSimulation.compute_final_stats()` counts actual events; `reconcile_likes_with_stats()` adjusts like events to target ratio then recouples stats.

### R13 — Popularity Normalizer (Expectation-Based)
**Rule**: Each video's Poisson lambda = its share of the **derived event budget**:
```
expected_events = users × sessions/user/yr × events/session  (default 48,000)
lambda_i = expected_events_per_video × weight_i × boost_i × variance_i / normalizer
```
`normalizer = mean(weight × boost × variance)` over uniform content-type mix → `sum(lambda_i) = expected_events` exactly, **no rescaling**.
**Enforcement**: `VideoSimulation._compute_popularity_normalizer()` divides by sampling-weighted mean = 1.0. `generate_videos()` emits uniform content-type mix so assumption holds.

### R15 — Single Plan/Pricing Table
**Rule**: All payment amounts derive from **one** `PLAN_PRICING` table in `PaymentFactory`. No hard-coded amounts elsewhere.
**Enforcement**: `PaymentFactory.get_amount(plan_type, duration_days)` is the only accessor. `attempt_no` (1-3) and `billing_reason` (initial/retry/renewal) validated at creation.

### R18 — Watch Time Bounds by Content Type
**Rule**: `watch_time_sec` bounds come from `VideoSimulation.WATCH_TIME_BOUNDS` per content type, capped by content duration.
**Enforcement**: `InteractionFactory._get_watch_time_bounds()` delegates to video simulation constants.

### R19 — Deterministic Replay
**Rule**: Single `RandomProvider(seed=42)` instance shared across **all** modules → bit-for-bit deterministic replay. `BusinessClock` single master cursor + forks for per-entity timelines.
**Enforcement**: Tests in `test_deterministic.py` verify identical seeds produce identical outputs.

---

## Phase Markers Used in Code Comments

Every `.py` file uses `# -- Phase X: name --` markers mapping directly to this clean 8-phase orchestration flow:

| Phase | Name | Verification Gate Context |
|-------|------|---------------------------|
| 1 | Initialize & Clock | Base timing anchors and shared RNG state (`clock.py`, `random_provider.py`) |
| 2 | PostgreSQL Domain | Core transactional schema and R1/R2 Verification (Gate I) |
| 3 | MongoDB Domain | Content setup and Match-Video consistency |
| 4 | Temporal Isolation | Global state mapping and video tracking (Gate II) |
| 5 | Session Topology | Engagement tiers and workload modeling |
| 6 | Kafka Event Pipeline | Event serialization and transaction limits (Gate III) |
| 7 | Anomaly Injection | Controlled distortion via shared RNG (Gate IV) |
| 8 | Bulk Loading Pipeline | Database ingestion and production orchestration |

---

## File Reference Map

| File | Primary Operating Phase | Core Spec Focus |
|------|-------------------------|-----------------|
| `clock.py` | 1 | Initialize & Clock |
| `random_provider.py` | 1 | Single RNG Master State (R19) |
| `id_generator.py` | 1 | Structural Unique Identity |
| `match_registry.py` | 1 | Unified Monotonic Match Blocks (R4) |
| `postgres/*.py` | 2 | PostgreSQL Transaction Domain |
| `invariants.py` | Cross-Cutting | Universal Progressive Quality Gates (I, II, III, IV) |
| `mongo/video_simulation.py` | 3 | MongoDB Asset Distribution (R5, R13) |
| `temporal_registry.py` | 4 | Global Availability Verification Map (R7) |
| `registry/billing_eligibility.py` | 4 | Temporal Contract Validation Gates (R6) |
| `registry/session_factory.py` | 5 | Stratified Engagement Session Creation (R11) |
| `kafka/*.py` | 6 | High-Throughput Event Serialization & Ceiling Bounds |
| `anomaly_coordinator.py` | 7 | Independent Statistical Control & Rates (3.5% Flat Target) |
| `dirty_data/*.py` | 7 | Micro-Mutation Pipeline Injectors (Gate IV Compliance) |
| `run_simulation.py` | Orchestration | Full 8-Phase Architectural Orchestrator |
---

## Running the Simulation

```bash
# Laptop-friendly default (1k users, 500 videos, 50k ceiling)
python -m scripts.simulation.run_simulation --dry-run

# Full scale (10k users, 10k videos, 100k ceiling)
python -m scripts.simulation.run_simulation --users 10000 --videos 10000 --events 100000

# Smoke test
python -m scripts.simulation.run_simulation --dry-run --users 50 --videos 50 --events 2400
```

## Testing

```bash
pytest tests/ -v
```
All tests must pass for Production Gold Baseline certification.