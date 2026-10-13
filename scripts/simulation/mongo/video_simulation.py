import random
from datetime import datetime, timedelta

from faker import Faker

from scripts.simulation.id_generator import IDGenerator
from scripts.simulation.random_provider import RandomProvider


faker = Faker()


class VideoSimulation:

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

    SUPER_CUP_TEAMS = [
        "Al_Hilal",
        "Al_Nassr",
        "Al_Ittihad",
        "Al_Ahli",
    ]

    COMPETITIONS = {
        "RSL": RSL_TEAMS,
        "Kings_Cup": KINGS_CUP_TEAMS,
        "Super_Cup": SUPER_CUP_TEAMS,
    }

    CONTENT_TYPES = (
        "live",
        "highlight",
        "replay",
    )

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

    STATUSES = (
        "streaming",
        "ended",
        "archived",
    )

    TAGS = (
        "football",
        "highlights",
        "live",
        "RSL",
        "Kings_Cup",
        "Super_Cup",
        "goals",
        "replay",
        "VAR",
    )

    LIVE_MATCH_DURATION_MIN = 105
    LIVE_MATCH_DURATION_MAX = 120

    def __init__(
        self,
        *,
        clock,
        random_provider: RandomProvider,
        simulation_start: datetime,
        simulation_end: datetime,
    ):
        self.clock = clock
        self.random = random_provider
        self.simulation_start = simulation_start
        self.simulation_end = simulation_end


    

    def create_video(self) -> dict:

            competition_id = self.random.choice(
                tuple(self.COMPETITIONS.keys())
            )

            team1, team2 = self.random.sample(
                self.COMPETITIONS[competition_id],
                k=2,
            )

            content_type = self.random.choice(
                self.CONTENT_TYPES
            )

            status = self.random.choice(
                self.STATUSES
            )

            created_at = (
                self.clock.now()
                - timedelta(
                    days=self.random.randint(0, 30),
                    hours=self.random.randint(0, 23),
                    minutes=self.random.randint(0, 59),
                )
            )

            if content_type == "highlight":
                duration = self.random.randint(
                    180,
                    600,
                )

            elif content_type == "replay":
                duration = self.random.randint(
                    6300,
                    7500,
                )

            else:  # live
                duration = None

            # Generate live match window for live content
            if content_type == "live":
                # Match starts within simulation window
                sim_window_days = (self.simulation_end - self.simulation_start).days
                match_start_offset_days = self.random.randint(0, max(0, sim_window_days - 1))
                match_start_offset_hours = self.random.randint(0, 23)
                match_start_offset_minutes = self.random.randint(0, 59)
                live_start_at = (
                    self.simulation_start
                    + timedelta(
                        days=match_start_offset_days,
                        hours=match_start_offset_hours,
                        minutes=match_start_offset_minutes,
                    )
                )
                match_duration_min = self.random.randint(
                    self.LIVE_MATCH_DURATION_MIN,
                    self.LIVE_MATCH_DURATION_MAX,
                )
                live_end_at = live_start_at + timedelta(minutes=match_duration_min)
            else:
                live_start_at = None
                live_end_at = None

            if status in ("ended", "archived"):
                last_updated = (
                    created_at
                    + timedelta(
                        minutes=self.random.randint(5, 150)
                    )
                )
            else:
                last_updated = created_at

            return {
                "video_id": IDGenerator.generate(),

                "content_type": content_type,

                "match_id": (
                    f"match_{self.random.randint(1000, 9999)}"
                ),

                "competition": {
                    "id": competition_id,
                    "name": competition_id.replace(
                        "_",
                        " ",
                    ),
                    "teams": [
                        team1,
                        team2,
                    ],
                },

                "title": (
                    f"{team1} vs {team2}"
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

                "duration_sec": duration,

                "technical": {
                    "max_resolution": self.random.choice(
                        self.RESOLUTIONS
                    ),
                    "codec": self.random.choice(
                        self.CODECS
                    ),
                },

                "status": status,

                "stats": {
                    "views": self.random.randint(
                        0,
                        500000,
                    ),
                    "likes": self.random.randint(
                        0,
                        50000,
                    ),
                },

                "created_at": created_at,

                "last_updated": last_updated,

                "live_start_at": live_start_at,

                "live_end_at": live_end_at,
            }

    def generate_videos(self, count: int):

            for _ in range(count):
                yield self.create_video()