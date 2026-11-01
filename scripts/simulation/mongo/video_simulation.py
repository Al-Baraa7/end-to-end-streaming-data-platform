"""Video Simulation — Phase 2: MongoDB Video Catalogue (R4/R5/R13)

Generates the simulated video catalogue for MongoDB.

R4 - match concept
    A match is registered exactly once (`MatchRegistry`, monotonic
    `match_000001`... sequence) together with its teams, competition and
    kick-off instant. Its live broadcast, its highlight reel and its full
    replay are three artifacts OF that match and therefore share that
    `match_id`, `kickoff_at`, `match_duration_sec` and `competition` block
    verbatim. The previous generator drew `randint(1000, 9999)` per video,
    which collided across matches and split single matches into unrelated ids.

R5 - temporal window integrity
    * `created_at <= live_start_at` - a broadcast is announced at or before
      kick-off;
    * `live_start_at + duration_sec <= simulation_end` - a broadcast window
      always fits inside the horizon (it is never truncated by it);
    * `duration_sec` is derived from the match window (live/replay = the
      match's own playtime, highlight = a clip of it), so playtime and
      broadcast window can no longer drift apart;
    * `status` is DERIVED from the artifact's window vs its own `created_at`
      and the horizon - it is never drawn from a free 3-way choice.

R13 - popularity normalizer
    The magnitude of a video's latent popularity (Poisson lambda) is NOT
    hard-coded here. It is the per-video share of the event budget derived from
    the viewing-session model (users x sessions/user/yr x events/session), passed
    in by the orchestrator as `expected_events_per_video`. These weights only
    shape how that expectation is distributed across content types, and they are
    normalised so that their sampling-weighted mean is exactly 1.0 - hence
    sum(latent_popularity) == expected_events_per_video x video_count, i.e. the
    derived budget, with no post-hoc rescaling of sampled lambdas.
See README.md#phase-markers-used-in-code-comments
"""

import math
import random
from datetime import datetime, timedelta, timezone

from faker import Faker

from ..id_generator import IDGenerator
from ..match_registry import Match, MatchRegistry
from ..random_provider import RandomProvider


faker = Faker()


class VideoSimulation:
    """
    Generates the simulated video catalogue for MongoDB.

    R4 - match concept
        A match is registered exactly once (`MatchRegistry`, monotonic
        `match_000001`... sequence) together with its teams, competition and
        kick-off instant. Its live broadcast, its highlight reel and its full
        replay are three artifacts OF that match and therefore share that
        `match_id`, `kickoff_at`, `match_duration_sec` and `competition` block
        verbatim. The previous generator drew `randint(1000, 9999)` per video,
        which collided across matches and split single matches into unrelated ids.

    R5 - temporal window integrity
        * `created_at <= live_start_at` - a broadcast is announced at or before
          kick-off;
        * `live_start_at + duration_sec <= simulation_end` - a broadcast window
          always fits inside the horizon (it is never truncated by it);
        * `duration_sec` is derived from the match window (live/replay = the
          match's own playtime, highlight = a clip of it), so playtime and
          broadcast window can no longer drift apart;
        * `status` is DERIVED from the artifact's window vs its own `created_at`
          and the horizon - it is never drawn from a free 3-way choice.
    """

    RSL_TEAMS = [
        "Al_Hilal",
        "Al_Nassr",
        "Al_Ittihad",
        "Al_Ahli",
        "Al_Ettifaq",
        "Al_Taawoun",
        "Al_Fateh",
        "Al_Fayha",
        "Al_Khaleej",
        "Al_Okhdood",
        "Al_Raed",
        "Al_Riyadh",
        "Al_Shabab",
        "Al_Wehda",
        "Damac",
        "Al_Qadsiah",
        "Al_Orobah",
        "Al_Kholood",
    ]

    KINGS_CUP_TEAMS = RSL_TEAMS + [
        "Al_Jabalain",
        "Al_Bukiryah",
        "Al_Jandal",
        "Al_Najma",
        "Al_Ain",
        "Al_Safa",
        "Al_Adalah",
        "Al_Batin",
        "Ohud",
        "Jeddah",
        "Al_Jubail",
        "Hajer",
        "Al_Shoalah",
        "Al_Faisaly",
    ]

    COMPETITIONS = {
        "RSL": RSL_TEAMS,
        "Kings_Cup": KINGS_CUP_TEAMS,
    }

    # Competition selection weights: 75% RSL, 25% Kings_Cup
    COMPETITION_WEIGHTS = {
        "RSL": 0.75,
        "Kings_Cup": 0.25,
    }

    CONTENT_TYPES = (
        "live",
        "highlight",
        "replay",
    )

    # Popularity STRUCTURE per content type - DIMENSIONLESS.
    #
    # R13: the magnitude of a video's latent popularity (Poisson lambda) is NOT
    # hard-coded here. It is the per-video share of the event budget derived from
    # the viewing-session model (users x sessions/user/yr x events/session), passed
    # in by the orchestrator as `expected_events_per_video`. These weights only
    # shape how that expectation is distributed across content types, and they are
    # normalised so that their sampling-weighted mean is exactly 1.0 - hence
    # sum(latent_popularity) == expected_events_per_video x video_count, i.e. the
    # derived budget, with no post-hoc rescaling of sampled lambdas.
    POPULARITY_WEIGHT_BY_TYPE = {
        "live": 3.0,
        "replay": 1.0,
        "highlight": 0.5,
    }

    # Expected events per video used when the orchestrator does not derive one.
    # Kept for backward compatibility of standalone/test construction only.
    DEFAULT_EXPECTED_EVENTS_PER_VIDEO = 100.0

    # RSL competition gets 3x boost (part of the popularity structure above)
    RSL_COMPETITION_ID = "RSL"
    RSL_MULTIPLIER = 3.0

    # Retained for backward compatibility: these are the *fallback* floors for
    # FINAL stats when the orchestrator passes no config-driven value (it passes
    # 0 = stats are an exact count of generated events). They are NOT applied to
    # the latent popularity lambda - a lambda floor would break the derived-budget
    # identity in POPULARITY_WEIGHT_BY_TYPE above.
    MIN_VIEWS = 100
    MIN_LIKES = 1

    # Optional floors for FINAL stats (after event coupling). These preserve the
    # class constants for backward compatibility, but run_simulation overrides
    # them with config-driven values (0 = stats are an exact event count).
    DEFAULT_MIN_STATS_VIEWS = MIN_VIEWS
    DEFAULT_MIN_STATS_LIKES = MIN_LIKES

    # Like-to-view ratio range (0.5% - 3%)
    LIKE_RATIO_MIN = 0.005
    LIKE_RATIO_MAX = 0.03

    LANGUAGES = (
        "ar",
        "en",
    )

    RESOLUTIONS = (
        "4k",
        "1080p",
        "720p",
        "480p",
    )

    CODECS = (
        "H.264",
        "H.265",
    )

    # Domain of `status` values. R5 - these are no longer SAMPLED: `_derive_status`
    # picks one deterministically from the artifact's temporal window, and binding
    # the three names keeps that derivation provably inside the declared domain.
    STATUSES = (
        "streaming",
        "ended",
        "archived",
    )
    STATUS_STREAMING, STATUS_ENDED, STATUS_ARCHIVED = STATUSES

    TAGS = (
        "football",
        "highlights",
        "live",
        "RSL",
        "Kings_Cup",
        "goals",
        "replay",
        "VAR",
    )

    # Match playtime in minutes (kick-off to final whistle).
    LIVE_MATCH_DURATION_MIN = 105
    LIVE_MATCH_DURATION_MAX = 120

    # Highlight reels are clips OF the match, so they are never longer than the
    # match window itself (R5: `duration_sec` is clamped to `match_duration_sec`).
    HIGHLIGHT_DURATION_MIN = 180
    HIGHLIGHT_DURATION_MAX = 600

    # Suffix that makes the three artifacts of one match distinguishable by title
    # while they keep sharing the exact same match metadata (R4).
    TITLE_SUFFIX_BY_TYPE = {
        "live": "",
        "highlight": " - Highlights",
        "replay": " - Full Match",
    }

    # Phase 5 - R18: Watch-time bounds per content type (seconds)
    # Used by interaction factory / session factory to validate watch_time_sec
    WATCH_TIME_BOUNDS = {
        "live": (1, 7200),       # Live: up to full match duration (2 hours)
        "highlight": (1, 600),   # Highlight: up to clip length (10 min)
        "replay": (1, 7200),     # Replay: up to full match duration (2 hours)
    }

    # Phase 5 - R18: Valid status/content_type combinations (invariant assertions)
    # Derived from R5 temporal rules:
    # - live: can be "streaming" (if live_end_at >= horizon) or "ended" (if live_end_at < horizon)
    # - highlight/replay (VOD): always "archived" (no broadcast window)
    VALID_STATUS_BY_CONTENT_TYPE = {
        "live": {"streaming", "ended"},
        "highlight": {"archived"},
        "replay": {"archived"},
    }

    def __init__(
        self,
        *,
        clock,
        random_provider: RandomProvider,
        simulation_start: datetime,
        simulation_end: datetime,
        min_stats_views: int | None = None,
        min_stats_likes: int | None = None,
        expected_events_per_video: float | None = None,
        match_registry: MatchRegistry | None = None,
    ):
        self.clock = clock
        self.random = random_provider
        self.simulation_start = simulation_start
        self.simulation_end = simulation_end

        # R4 - the registry that owns `match_id`s and the match catalogue. The
        # orchestrator passes one in so it can inspect the generated schedule; a
        # private registry is created when this class is used standalone.
        self.match_registry = (
            match_registry if match_registry is not None else MatchRegistry()
        )

        # R13 - demand basis for event generation: the number of events a video
        # of AVERAGE popularity is expected to attract, derived by the orchestrator
        # from the viewing-session model:
        #     expected_events_per_video = users x sessions/user/yr x events/session
        #                                 / video_count
        # Passing it in (rather than hard-coding view counts here) is what keeps
        # sum(latent_popularity) == the derived event budget, so event generation
        # never needs a rescaling pass.
        if expected_events_per_video is None:
            expected_events_per_video = self.DEFAULT_EXPECTED_EVENTS_PER_VIDEO
        expected_events_per_video = float(expected_events_per_video)
        if expected_events_per_video <= 0:
            raise ValueError(
                "expected_events_per_video must be > 0, got "
                f"{expected_events_per_video}"
            )
        self.expected_events_per_video = expected_events_per_video

        # Deterministic normaliser (declared weights only, never sampled data) that
        # makes the sampling-weighted mean of the popularity structure exactly 1.0.
        self._popularity_normalizer = self._compute_popularity_normalizer()

        # Config-driven floors for the final (coupled) stats.
        # None keeps the class defaults for backward compatibility.
        self.min_stats_views = (
            self.DEFAULT_MIN_STATS_VIEWS
            if min_stats_views is None
            else min_stats_views
        )
        self.min_stats_likes = (
            self.DEFAULT_MIN_STATS_LIKES
            if min_stats_likes is None
            else min_stats_likes
        )

    # ------------------------------------------------------------------
    # R4 - the match concept: one match, one set of metadata, many artifacts
    # ------------------------------------------------------------------

    def create_match(self) -> Match:
        """
        Register ONE match: competition, teams, kick-off and playtime.

        The match window is drawn so that it always fits inside the simulation
        horizon (R5: `kickoff_at + duration_sec <= simulation_end`), which is what
        makes `created_at <= live_start_at`, the window-bounded `duration_sec` and
        the derived `status` achievable for every artifact built from it.
        """
        competition_ids = list(self.COMPETITIONS.keys())
        competition_id = self.random.choices(
            population=competition_ids,
            weights=[self.COMPETITION_WEIGHTS[c] for c in competition_ids],
            k=1,
        )[0]

        home_team, away_team = self.random.sample(
            self.COMPETITIONS[competition_id],
            k=2,
        )

        duration_sec = (
            self.random.randint(
                self.LIVE_MATCH_DURATION_MIN,
                self.LIVE_MATCH_DURATION_MAX,
            )
            * 60
        )

        # R5: the whole match window must close at or before the horizon. Draw the
        # kick-off inside [simulation_start, simulation_end - duration_sec] and clamp
        # the draw, so a match can never be left running past the end of the run.
        latest_kickoff = self.simulation_end - timedelta(seconds=duration_sec)
        if latest_kickoff <= self.simulation_start:
            raise ValueError(
                "the simulation window is too short to host a match: "
                f"{self.simulation_start} + {duration_sec}s > {self.simulation_end}"
            )

        available_days = (latest_kickoff - self.simulation_start).days
        # Phase 5 - R18: Fix randint off-by-one on video window.
        # randint(a, b) is inclusive on both ends. We need [0, available_days - 1] for
        # full days, because day `available_days` would be the (available_days+1)-th day.
        max_day_offset = max(0, available_days - 1) if available_days > 0 else 0
        kickoff_at = self.simulation_start + timedelta(
            days=self.random.randint(0, max_day_offset),
            hours=self.random.randint(0, 23),
            minutes=self.random.randint(0, 59),
        )
        # The day/hour/minute draw can overshoot `latest_kickoff`; clamping keeps the
        # window inside the horizon (and is what occasionally parks a final whistle
        # exactly on the horizon, i.e. a live video still "streaming" when the
        # catalogue is read).
        kickoff_at = min(kickoff_at, latest_kickoff)

        return self.match_registry.register(
            competition_id=competition_id,
            competition_name=competition_id.replace("_", " "),
            home_team=home_team,
            away_team=away_team,
            kickoff_at=kickoff_at,
            duration_sec=duration_sec,
        )

    def _derive_status(
        self,
        *,
        created_at: datetime,
        live_start_at: datetime | None,
        live_end_at: datetime | None,
    ) -> str:
        """
        R5 - DERIVE `status` from the artifact's temporal window (never sample it).

        `status` used to be `self.random.choice(self.STATUSES)`, a free 3-way
        choice: a highlight could claim to be "streaming", a broadcast scheduled
        next month could be "ended", and a finished match could stay "streaming"
        forever. It is now a pure function of the two reference instants that
        actually describe the artifact:

        * its own `created_at` - when the document was published, and
        * the horizon `simulation_end` - when the catalogue is read, i.e. the
          instant a consumer sees the row.

        Rules
        -----
        * highlight / replay - no broadcast window at all -> the archive; a VOD row
          can never be "streaming" (`archived`).
        * live whose window is still open at the horizon -> `streaming`: the final
          whistle coincides with the horizon, so the broadcast is on air at the
          instant the catalogue is read.
        * live published only after its own final whistle -> `archived`: nothing is
          left to broadcast. The `created_at <= live_start_at` constructor guard
          makes this unreachable for generated data, but it keeps the derivation
          total (a defined state for every possible input).
        * live announced at or before kick-off and off air before the horizon ->
          `ended`: the broadcast ran its full course inside the run.
        """
        if live_start_at is None or live_end_at is None:
            return self.STATUS_ARCHIVED

        if self.simulation_end <= live_end_at:
            return self.STATUS_STREAMING

        if live_end_at <= created_at:
            return self.STATUS_ARCHIVED

        return self.STATUS_ENDED

    def create_video(
        self,
        match: Match | None = None,
        content_type: str | None = None,
    ) -> dict:
        """
        Build ONE artifact (live broadcast / highlight reel / full replay) of a match.

        R4 - `match` is registered once by `create_match()` and all of its artifacts
        are built from that same record, so `match_id`, `kickoff_at`,
        `match_duration_sec` and the `competition` block are identical for the live
        feed, the highlight and the replay: they are literally that match's
        metadata. The id is never drawn here - it comes from `MatchRegistry`'s
        monotonic sequence.

        R5 - the artifact's timestamps are pinned to the match window:
        * live: broadcast window == the match window, `created_at` at or before
          kick-off, and the window closes at or before `simulation_end`;
        * VOD:  no broadcast window (the registry serves VOD from `created_at`),
          published between the final whistle and the horizon;
        * `duration_sec`: the match's own playtime for live/replay; for a highlight
          the clip length, clamped to the playtime so a clip can never outrun the
          match it was cut from;
        * `status`: DERIVED from the window vs `created_at` and the horizon (see
          `_derive_status`) - never sampled.

        Args:
            match: the match this artifact belongs to; created on the fly when None
                (keeps `create_video()` usable standalone, e.g. from notebooks).
            content_type: "live", "highlight" or "replay"; sampled uniformly when
                None.
        """
        if match is None:
            match = self.create_match()

        if content_type is None:
            content_type = self.random.choice(self.CONTENT_TYPES)
        if content_type not in self.CONTENT_TYPES:
            raise ValueError(
                f"unknown content_type {content_type!r}; expected one of "
                f"{self.CONTENT_TYPES}"
            )

        # ---------------- R5: pin the artifact to the match window ----------------
        match_start_at = match.kickoff_at
        match_end_at = match.end_at

        if content_type == "live":
            # The broadcast window IS the match window and `duration_sec` is its
            # length, so playtime and window can never drift apart (R5).
            live_start_at = match_start_at
            live_end_at = match_end_at
            duration_sec = match.duration_sec

            # R5: created_at <= live_start_at. A broadcast is announced ahead of
            # kick-off - never published inside or after it - while staying anchored
            # inside the simulation window.
            #
            # R13 note: TemporalRegistry serves a timestamp only when
            # `created_at <= ts <= live_end_at`, so a video published after its own
            # kick-off could never receive a single event; every Poisson draw it was
            # given was lost in placement.
            created_at = max(
                self.simulation_start,
                match_start_at - timedelta(minutes=self.random.randint(5, 1440)),
            )
        else:
            # VOD artifacts (highlight/replay) have no broadcast window: the registry
            # serves them from `created_at` onwards. They can only exist once the
            # match has been PLAYED, so they are published between the final whistle
            # and the horizon.
            live_start_at = None
            live_end_at = None
            created_at = min(
                match_end_at + timedelta(hours=self.random.randint(1, 72)),
                self.simulation_end,
            )

            if content_type == "highlight":
                # A highlight is a clip OF the match, so it is never longer than it.
                duration_sec = min(
                    self.random.randint(
                        self.HIGHLIGHT_DURATION_MIN,
                        self.HIGHLIGHT_DURATION_MAX,
                    ),
                    match.duration_sec,
                )
            else:  # replay
                # A replay IS the full match, i.e. exactly the match window.
                duration_sec = match.duration_sec

        # R5: `status` is DERIVED from the temporal window - never sampled.
        status = self._derive_status(
            created_at=created_at,
            live_start_at=live_start_at,
            live_end_at=live_end_at,
        )

        # Phase 5 - R18: Invariant assertion on status/content_type combinations
        valid_statuses = self.VALID_STATUS_BY_CONTENT_TYPE.get(content_type, set())
        if status not in valid_statuses:
            raise ValueError(
                f"Invalid status '{status}' for content_type '{content_type}'. "
                f"Valid statuses: {sorted(valid_statuses)}"
            )

        if status == self.STATUS_STREAMING:
            # Still on air at the read instant, so the last heartbeat is the horizon.
            last_updated = self.simulation_end
        else:
            last_updated = min(
                created_at + timedelta(minutes=self.random.randint(5, 150)),
                self.simulation_end,
            )

        # Compute latent popularity (Poisson lambda) for this video.
        # This is the EXPECTED view count that will drive event generation.
        # Actual stats will be computed from generated events (coupling).
        latent_popularity = self._compute_latent_popularity(
            content_type=content_type,
            competition_id=match.competition_id,
        )

        return {
            "video_id": IDGenerator.generate(),

            "content_type": content_type,

            # --- R4: match metadata, shared verbatim with the match's other
            # artifacts (live / highlight / replay). The id comes from the
            # registry's monotonic sequence, never from a random draw.
            "match_id": match.match_id,
            "kickoff_at": match.kickoff_at,
            "match_duration_sec": match.duration_sec,

            # A FRESH competition dict per artifact: the three artifacts must AGREE
            # on the competition, but must not share one mutable object (editing one
            # document must never rewrite its siblings).
            "competition": match.competition_metadata(),

            "title": (
                f"{match.home_team} vs {match.away_team}"
                f"{self.TITLE_SUFFIX_BY_TYPE[content_type]}"
            ),

            "description": faker.text(
                max_nb_chars=150
            ),

            "languages": self.random.sample(
                self.LANGUAGES,
                k=self.random.randint(1, 2),
            ),

            "tags": self.random.sample(
                self.TAGS,
                k=self.random.randint(2, 4),
            ),

            "duration_sec": duration_sec,

            "technical": {
                "max_resolution": self.random.choice(
                    self.RESOLUTIONS
                ),
                "codec": self.random.choice(
                    self.CODECS
                ),
            },

            "status": status,

            # Latent popularity parameter (lambda for Poisson sampling).
            # This couples video popularity model to event generation.
            "latent_popularity": latent_popularity,

            # Placeholder stats - will be OVERWRITTEN after event generation
            # to ensure perfect stats↔events coupling.
            "stats": {
                "views": latent_popularity,  # temporary, replaced post-events
                "likes": max(1, int(latent_popularity * 0.01)),
            },

            "created_at": created_at,

            "last_updated": last_updated,

            "live_start_at": live_start_at,

            "live_end_at": live_end_at,
        }

    def _compute_popularity_normalizer(self) -> float:
        """
        Expected value of POPULARITY_WEIGHT_BY_TYPE x competition boost under the
        exact sampling distribution used by create_video() (uniform content type,
        COMPETITION_WEIGHTS competition).

        Dividing each video's expectation by this normaliser keeps
        sum(latent_popularity) == expected_events_per_video x video_count == the
        derived event budget (R13). It is a property of the DECLARED model, never
        of the sampled draws, so it is identical on every run.
        """
        type_mean = sum(
            self.POPULARITY_WEIGHT_BY_TYPE.get(content_type, 1.0)
            for content_type in self.CONTENT_TYPES
        ) / len(self.CONTENT_TYPES)

        competition_mean = sum(
            weight * (
                self.RSL_MULTIPLIER
                if competition_id == self.RSL_COMPETITION_ID
                else 1.0
            )
            for competition_id, weight in self.COMPETITION_WEIGHTS.items()
        )

        normalizer = type_mean * competition_mean
        if normalizer <= 0:
            raise ValueError(
                "popularity normaliser must be > 0; check "
                "POPULARITY_WEIGHT_BY_TYPE and COMPETITION_WEIGHTS"
            )
        return normalizer

    def _compute_latent_popularity(
        self,
        *,
        content_type: str,
        competition_id: str,
    ) -> int:
        """
        Compute the latent popularity (Poisson lambda) for a video: the number of
        EVENTS this video is expected to attract over the simulation horizon.

        R13 model (demand side, never rescaled):
            lambda = expected_events_per_video     # from the viewing-session model
                     x popularity weight           # content type structure
                     x competition boost           # RSL x RSL_MULTIPLIER
                     x uniform(0.5, 1.5)           # mean-1.0 symmetric jitter
                     / normaliser                  # weighted mean of structure = 1.0

        Because the jitter has mean 1.0 and the structure is normalised, the lambdas
        sum to the derived budget in expectation with no rescaling pass. A lambda is
        only ever BOUNDED later - by the video's eligible-user capacity in
        run_simulation.py step [6] - and never scaled down to fit the --events
        ceiling (which is a ceiling, not a quota).
        """
        weight = self.POPULARITY_WEIGHT_BY_TYPE.get(content_type, 1.0)
        boost = (
            self.RSL_MULTIPLIER
            if competition_id == self.RSL_COMPETITION_ID
            else 1.0
        )

        # uniform(0.5, 1.5) has mean 1.0, so E[lambda] is preserved exactly.
        variance = self.random.uniform(0.5, 1.5)

        # No artificial floor: a lambda floor would break the derived-budget
        # identity above. Final-stat floors (config-driven, default 0 = exact
        # event coupling) are applied in compute_final_stats() instead.
        # int() truncation moves the expectation by < 1 event per video.
        return int(
            self.expected_events_per_video
            * weight
            * boost
            * variance
            / self._popularity_normalizer
        )

    def compute_final_stats(self, video: dict, events: list) -> dict:
        """
        Compute FINAL stats from actual generated events.
        This ensures perfect stats↔events coupling.
        
        Args:
            video: Video document dict
            events: List of InteractionEntity objects for this video
            
        Returns:
            Updated video dict with coupled stats
        """
        # Count actual interaction types
        play_events = [e for e in events if e.interaction_type == "play"]
        like_events = [e for e in events if e.interaction_type == "like"]
        
        # Views = play events (each play = one view)
        views = len(play_events)
        
        # Likes = like events
        likes = len(like_events)
        
        # Enforce config-driven floors (0 by default = exact event coupling,
        # i.e. no phantom views that were never produced by an event)
        views = max(views, self.min_stats_views)
        likes = max(likes, self.min_stats_likes)

        # Enforce likes <= views invariant (0 events must stay 0/0)
        if likes >= views:
            likes = max(0, views - 1)
        
        # Update video stats
        video["stats"] = {
            "views": views,
            "likes": likes,
        }
        
        return video

    def generate_videos(self, count: int):
        """
        Yield `count` artifacts, GROUPED BY MATCH (R4).

        One match is registered per group and yields its three artifacts in
        `CONTENT_TYPES` order (live, highlight, replay), so the live feed, the
        highlight reel and the replay of a match sit next to each other in the
        catalogue and share `match_id`, `kickoff_at`, `match_duration_sec` and the
        `competition` block by construction - they are not three independently
        generated videos that happen to be related.

        `count` stays a hard CEILING, not a quota per match: when it does not
        divide evenly by the trio size the final match simply contributes fewer
        artifacts (e.g. a replay that is not published yet). That is legal -
        `assert_match_artifacts_unified` requires the artifacts that DO exist to
        agree with each other, never that a match has all three.

        Because the trio is emitted in a fixed order, the catalogue content-type
        mix differs by at most one artifact per type. That is exactly the uniform
        content-type assumption `_compute_popularity_normalizer()` divides by, so
        the derived event budget (R13) keeps being spread over the catalogue with
        no rescaling.

        Args:
            count: number of artifacts to yield (must be > 0).
        """
        if count <= 0:
            raise ValueError(f"count must be > 0, got {count}")

        emitted = 0
        while emitted < count:
            match = self.create_match()

            for content_type in self.CONTENT_TYPES:
                if emitted >= count:
                    return

                yield self.create_video(
                    match=match,
                    content_type=content_type,
                )

                emitted += 1