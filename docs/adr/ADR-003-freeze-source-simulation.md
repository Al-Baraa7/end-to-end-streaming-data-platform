ADR — Freeze Source Simulation and Continue with Silver/Gold

Context

The original data-generation system became a major blocker before reaching the Gold layer. As more parts of the pipeline were built, problems in the generated data kept appearing, which led to repeated fixes and reruns without reaching a stable point.

To improve this, the generation system was redesigned around a human-behavior simulation model. This significantly improved the realism and consistency of the generated data, including user lifecycles, subscriptions, payments, interactions, timestamps, relationships, and event generation.

However, several source-level issues still remain. Some are intentional dirty data, while others are simulation rules that still need refinement. Continuing to fix every source issue immediately would risk repeating the same cycle: fix one problem, discover another, regenerate everything, and lose progress on the actual pipeline.

Main Issues Encountered and Addressed

- User lifecycle and timestamps
  
  - Ensured user creation and login events follow the simulated business clock.
  - Identified and isolated intentionally invalid login timestamps.

- Subscription lifecycle
  
  - Added realistic subscription generation and lifecycle relationships.
  - Identified invalid subscription date ranges as source-level data issues.

- Payment lifecycle
  
  - Implemented payment chains with retry attempts.
  - Ensured payments remain associated with their subscriptions and respect subscription timing.

- Interaction generation
  
  - Moved interaction generation into the user/subscription context instead of generating Kafka events independently after PostgreSQL generation.
  - Ensured interaction timestamps respect user, subscription, and video creation times.
  - Standardized the interaction model around "like", "play", "complete", and "pause".

- Watch-time semantics
  
  - Identified that the original fixed watch-time limits did not correctly represent the meaning of "play" and "complete".
  - The intended final semantics are:
    - "play" → partial viewing
    - "complete" → full viewing
    - "like" / "pause" → no watch time
  - The final source adjustment is intentionally deferred.

- Video generation
  
  - Fixed generator/list handling and issues caused by incorrect class/method structure.
  - Identified intentionally invalid video durations and other dirty records without treating them as source-generation failures.

- Kafka
  
  - Fixed the relationship between generated users, videos, and Kafka interactions.
  - Identified invalid timestamps, interaction types, devices, and watch-time values as data-quality cases requiring proper downstream handling.
  - Deferred duration-dependent watch-time validation because video duration is not currently available in the Kafka transformation context.

- End-to-end temporal consistency
  
  - Verified that generated Kafka events remain within valid subscription periods and do not occur before the related user or video.
  - Removed the previous design where Kafka events were generated independently of the subscription lifecycle.

Decision

The current source dataset is frozen while Silver and Gold are being built.

Silver and Gold will be implemented according to the correct business rules and intended data semantics, not according to every inconsistency currently present in the source data.

Any source issue that conflicts with the intended Gold/business rules will be documented rather than immediately fixed.

Once the full pipeline has been built, all documented source-level fixes will be applied together. The source data will then be regenerated once, and the complete pipeline will be executed end-to-end.

This avoids repeatedly rebuilding the source layer while the downstream architecture is still being developed and keeps the focus on building the pipeline correctly.

Consequences

- Silver and Gold can continue without being blocked by source-generation imperfections.
- Known source issues remain explicitly documented rather than forgotten.
- Gold logic remains aligned with the intended business semantics.
- Source corrections are consolidated into one final regeneration before the full pipeline run.
- The current dataset should not be treated as the final production-quality source dataset.