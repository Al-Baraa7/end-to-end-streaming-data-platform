"""MatchRegistry — Phase 2: Identity & Randomness (R4)

Single source of truth for match catalogue. Monotonic `match_000001...` sequence eliminates
collisions and ensures live/highlight/replay share identical match metadata.
See README.md#phase-markers-used-in-code-comments
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterator, Tuple


@dataclass(frozen=True, slots=True)
class Match:
    """Immutable description of one football match - the shared R4 anchor."""

    match_id: str
    competition_id: str
    competition_name: str
    home_team: str
    away_team: str
    kickoff_at: datetime
    duration_sec: int

    @property
    def end_at(self) -> datetime:
        """The final whistle: kick-off plus the match's own duration."""
        return self.kickoff_at + timedelta(seconds=self.duration_sec)

    @property
    def teams(self) -> Tuple[str, str]:
        """(home, away) - the team pair shared by every artifact of the match."""
        return (self.home_team, self.away_team)

    def competition_metadata(self) -> dict:
        """
        The ``competition`` sub-document of every artifact of this match.

        A FRESH dict (and a fresh ``teams`` list) is returned on every call: the
        artifacts of one match must agree by VALUE, never share one mutable
        object, so a later edit to one document cannot silently rewrite its
        siblings.
        """
        return {
            "id": self.competition_id,
            "name": self.competition_name,
            "teams": [self.home_team, self.away_team],
        }

    def shared_metadata(self) -> dict:
        """R4 - exactly the metadata every artifact of this match must carry."""
        return {
            "match_id": self.match_id,
            "kickoff_at": self.kickoff_at,
            "match_duration_sec": self.duration_sec,
            "competition": self.competition_metadata(),
        }


class MatchRegistry:
    """
    Hands out collision-free ``match_id``s and owns the match catalogue.

    Usage::

        registry = MatchRegistry()
        match = registry.register(
            competition_id="RSL",
            competition_name="RSL",
            home_team="Al_Hilal",
            away_team="Al_Nassr",
            kickoff_at=kickoff,      # timezone-aware
            duration_sec=6600,
        )
        assert registry.get(match.match_id) is match
    """

    ID_PREFIX = "match_"
    ID_WIDTH = 6  # match_000001 - zero padded so lexical order == numeric order

    def __init__(self, *, start_sequence: int = 1):
        """
        Args:
            start_sequence: first sequence number to hand out (>= 1). Pass a value
                above the highest id already registered elsewhere to keep the
                sequence collision-free across runs without any randomness.
        """
        if int(start_sequence) < 1:
            raise ValueError(f"start_sequence must be >= 1, got {start_sequence!r}")
        self._next_sequence = int(start_sequence)
        self._matches: Dict[str, Match] = {}

    # -- Phase 2: Registration --

    def register(
        self,
        *,
        competition_id: str,
        competition_name: str,
        home_team: str,
        away_team: str,
        kickoff_at: datetime,
        duration_sec: int,
    ) -> Match:
        """Register one match and return it carrying its freshly minted id."""
        if not competition_id:
            raise ValueError("competition_id is required")
        if not competition_name:
            raise ValueError("competition_name is required")
        if not home_team or not away_team:
            raise ValueError("both teams are required")
        if home_team == away_team:
            raise ValueError(
                f"a match needs two distinct teams, got {home_team!r} twice"
            )
        if not isinstance(kickoff_at, datetime):
            raise ValueError(
                f"kickoff_at must be a datetime, got {type(kickoff_at).__name__}"
            )
        if kickoff_at.tzinfo is None:
            raise ValueError("kickoff_at must be timezone-aware")
        if int(duration_sec) <= 0:
            raise ValueError(f"duration_sec must be > 0, got {duration_sec!r}")

        match_id = f"{self.ID_PREFIX}{self._next_sequence:0{self.ID_WIDTH}d}"
        self._next_sequence += 1

        match = Match(
            match_id=match_id,
            competition_id=competition_id,
            competition_name=competition_name,
            home_team=home_team,
            away_team=away_team,
            kickoff_at=kickoff_at,
            duration_sec=int(duration_sec),
        )
        self._matches[match_id] = match
        return match

    # -- Phase 2: Introspection --

    def get(self, match_id: str) -> Match:
        """Return a registered match; raises KeyError when the id is unknown."""
        return self._matches[match_id]

    def matches(self) -> Tuple[Match, ...]:
        """All registered matches, in registration (sequence) order."""
        return tuple(self._matches.values())

    def ids(self) -> Tuple[str, ...]:
        """All registered ids, in registration (sequence) order."""
        return tuple(self._matches.keys())

    @property
    def next_sequence(self) -> int:
        """Sequence number the next registration will consume."""
        return self._next_sequence

    def __len__(self) -> int:
        return len(self._matches)

    def __contains__(self, match_id: object) -> bool:
        return match_id in self._matches

    def __iter__(self) -> Iterator[Match]:
        return iter(self._matches.values())
