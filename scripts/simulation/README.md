# Simulation Engine — Unified Documentation

**Purpose**: Generate a deterministic, referentially-consistent dataset for a football-video streaming platform across PostgreSQL (users/subscriptions/payments), MongoDB (videos), and Kafka (interaction events).

**Execution Topology**: 9-phase pipeline with strict topological ordering:
1. **Initialize** — `BusinessClock` (UTC, seed=42), `RandomProvider` (single instance), `MatchRegistry` (monotonic `match_000001...`)
2. **PostgreSQL** — Users → Subscriptions → Payments (R2 non-overlapping, R1 horizon clamping, R6/R7 billing gate)
3. **MongoDB** — Videos grouped by match (R4 match concept, R5 temporal integrity, R13 popularity normalizer)
4. **TemporalRegistry** — Load subscriptions+videos, apply R7 paid-subscription gate, build eligibility map
5. **Anomaly Coordinator** — Register volumes, calculate per-stream rates (target 3.5% blended, bounds 1%-5%)
6. **Session Factory** — Assign engagement tiers, generate sessions clustered around live/VOD windows
7. **Event Generation** — Flatten sessions to events, enforce Kafka event ceiling (not quota)
8. **Dirty Injection** — Stream deepcopy micro-mutations at coordinator rates (pristine masters preserved)
9. **Load/Produce** — PostgreSQL bulk load, MongoDB batch insert, Kafka producer drain

---

## Glossary: Logical Name → Real Meaning

| Logical Name | Real Meaning |
|--------------|--------------|
| `SIM_KAFKA_EVENT_CEILING` | Safety ceiling on total Kafka events (50,000 default). The **derived target** is `users × sessions/user/yr × events/session` (default 48,000). Realized events ≤ ceiling; landing under target is legitimate Poisson noise, never rescaled. |
| `KAFKA_EVENT_CEILING` | Local alias for the ceiling in `run_simulation.py`. |
| `calculate_rates()` | Calculates volume-weighted blended anomaly rate of 3.5% (strictly bounded 1%-5%). Larger streams get rates closer to target, weighted average = 3.5%. |
| `reconcile_likes_with_stats()` | Tier-aware like reconciliation. Adjusts likes within each engagement tier: HIGH 2-3%, MEDIUM 1-2%, LOW 0.5-1%, while maintaining `stats.likes == like_events`. |
| `SessionFactory(frozen_eligibility_snapshot)` | Receives immutable eligibility snapshot (not mutable registry) for parallelization safety. |

## Anomaly Rate

Volume-weighted blended anomaly rate of 3.5% (strictly bounded 1%-5%) across all entity streams. Larger streams get rates closer to the target, smaller streams may vary slightly but the weighted average hits exactly the target blended rate.

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

Every `.py` file uses `# -- Phase X: name --` markers matching this topology:

| Phase | Name | Modules |
|-------|------|---------|
| 1 | Temporal Isolation | `clock.py`, `temporal_registry.py` |
| 2 | Identity & Randomness | `id_generator.py`, `random_provider.py`, `match_registry.py` |
| 3 | PostgreSQL Domain | `postgres/*.py`, `registry/billing_eligibility.py` |
| 4 | MongoDB Domain | `mongo/video_simulation.py` |
| 5 | Anomaly Coordination | `anomaly_coordinator.py`, `dirty_data/*.py` |
| 6 | Session Topology | `registry/session_factory.py` |
| 7 | Kafka Event Pipeline | `kafka/*.py` |
| 8 | Orchestration | `run_simulation.py` |
| 9 | Invariants & Validation | `invariants.py` |
**Why**: Previous `randint(1000,9999)` per video caused collisions (birthday paradox) and split single matches into unrelated IDs.
---

## File Reference Map

| File | Primary Phase | See README Section |
|------|---------------|-------------------|
| `clock.py` | 1 | Temporal Isolation |
| `id_generator.py` | 2 | Identity & Randomness |
| `random_provider.py` | 2 | Identity & Randomness |
| `match_registry.py` | 2 | Match Concept (R4) |
| `temporal_registry.py` | 1 | Eligibility Gate (R7) |
| `anomaly_coordinator.py` | 5 | Anomaly Coordination |
| `dirty_data/postgres_dirty_injection.py` | 5 | Anomaly Coordination |
| `dirty_data/mongo_dirty_injection.py` | 5 | Anomaly Coordination |
| `dirty_data/kafka_dirty_injection.py` | 5 | Anomaly Coordination |
| `postgres/user_factory.py` | 3 | PostgreSQL Domain |
| `postgres/user_lifecycle.py` | 3 | PostgreSQL Domain (R8) |
| `postgres/subscription_factory.py` | 3 | PostgreSQL Domain (R1, R2) |
| `postgres/payment_factory.py` | 3 | PostgreSQL Domain (R6, R15) |
| `postgres/user_entity.py` | 3 | PostgreSQL Domain |
| `postgres/subscription_entity.py` | 3 | PostgreSQL Domain (R1) |
| `postgres/payment_entity.py` | 3 | PostgreSQL Domain (R15) |
| `postgres/user_simulation.py` | 3 | PostgreSQL Domain |
| `postgres/postgres_simulation.py` | 3 | PostgreSQL Domain (R6/R7 gate) |
| `mongo/video_simulation.py` | 4 | MongoDB Domain (R4, R5, R13) |
| `kafka/interaction_factory.py` | 7 | Kafka Event Pipeline |
| `kafka/video_events_producer.py` | 7 | Kafka Event Pipeline |
| `kafka/interaction_entity.py` | 7 | Kafka Event Pipeline |
| `registry/billing_eligibility.py` | 3 | Billing Eligibility (R6/R7) |
| `registry/session_factory.py` | 6 | Session Topology (R11) |
| `invariants.py` | 9 | All Invariant Checkers |
| `run_simulation.py` | 8 | Orchestration (R13, R19) |

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