"""Кытчи: deterministic, explainable and social event ranking.

The old implementation assigned opaque demographic-like ``status_ml`` labels to
people. This module stores only explicit interests and observed event actions.
It does not infer identity, personality, age, or social status.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import exp, log1p


ACTION_WEIGHT = {
    "open": 0.18,
    "like": 0.85,
    "save": 1.2,
    "attend": 1.8,
    "dismiss": -1.4,
}


@dataclass(frozen=True)
class Candidate:
    id: int
    category: str
    tags: tuple[str, ...]
    starts_at: datetime
    popularity: int = 0
    is_free: bool = False
    venue_name: str = ""
    friend_going_count: int = 0
    source: str = ""
    created_at: datetime | None = None

    @property
    def features(self) -> set[str]:
        return {self.category.casefold(), *(tag.casefold() for tag in self.tags)}


@dataclass(frozen=True)
class HistoryItem:
    action: str
    occurred_at: datetime
    category: str
    tags: tuple[str, ...]


@dataclass
class RankedCandidate:
    candidate: Candidate
    score: float
    reasons: list[str] = field(default_factory=list)


def _ensure_aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def build_preference_weights(
    explicit_interests: list[str],
    history: list[HistoryItem],
    now: datetime,
) -> dict[str, float]:
    """Build a feature map with exponential time decay and no user labels."""
    weights: defaultdict[str, float] = defaultdict(float)
    for interest in explicit_interests:
        normalized = interest.strip().casefold()
        if normalized:
            weights[normalized] += 1.25

    aware_now = _ensure_aware(now)
    for item in history:
        age_days = max(0.0, (aware_now - _ensure_aware(item.occurred_at)).total_seconds() / 86400)
        decay = exp(-age_days / 60.0)
        value = ACTION_WEIGHT.get(item.action, 0.0) * decay
        for feature in {item.category.casefold(), *(tag.casefold() for tag in item.tags)}:
            weights[feature] += value
    return dict(weights)


def rank_candidates(
    candidates: list[Candidate],
    explicit_interests: list[str],
    history: list[HistoryItem],
    *,
    now: datetime | None = None,
    limit: int = 12,
) -> list[RankedCandidate]:
    """Rank for personal relevance, social proof and timing, then diversify."""
    now = _ensure_aware(now or datetime.now(timezone.utc))
    preferences = build_preference_weights(explicit_interests, history, now)
    max_popularity = max((item.popularity for item in candidates), default=1) or 1
    scored: list[RankedCandidate] = []

    for candidate in candidates:
        hours_until = (_ensure_aware(candidate.starts_at) - now).total_seconds() / 3600
        if hours_until < -2:
            continue

        matches = sorted(
            ((feature, preferences.get(feature, 0.0)) for feature in candidate.features),
            key=lambda item: item[1],
            reverse=True,
        )
        positive_matches = [(feature, weight) for feature, weight in matches if weight > 0]
        preference_score = sum(weight for _, weight in matches) / max(1.0, len(candidate.features) ** 0.5)
        popularity_score = log1p(max(0, candidate.popularity)) / log1p(max_popularity)
        friend_score = min(1.0, log1p(candidate.friend_going_count) / log1p(4))

        # Prefer events 1-30 days away without burying same-day discoveries.
        days_until = max(0.0, hours_until / 24)
        timing_score = exp(-abs(days_until - 7.0) / 28.0)
        exploration_bonus = 0.14 if not positive_matches else 0.0
        community_bonus = 0.06 if candidate.source == "community" else 0.0
        freshness_bonus = 0.0
        if candidate.created_at is not None:
            age_days = max(
                0.0,
                (now - _ensure_aware(candidate.created_at)).total_seconds() / 86400,
            )
            freshness_bonus = 0.06 * exp(-age_days / 21.0)
        score = (
            0.48 * preference_score
            + 0.17 * friend_score
            + 0.14 * popularity_score
            + 0.12 * timing_score
            + exploration_bonus
            + community_bonus
            + freshness_bonus
        )

        reasons: list[str] = []
        if positive_matches:
            reasons.append(f"Совпадает с интересом «{positive_matches[0][0]}»")
        if candidate.friend_going_count:
            word = "друг" if candidate.friend_going_count == 1 else "друга"
            reasons.append(f"Собираются {candidate.friend_going_count} {word}")
        if candidate.popularity and popularity_score >= 0.65:
            reasons.append("Популярно у зрителей")
        if days_until <= 2:
            reasons.append("Скоро начинается")
        elif not reasons:
            reasons.append("Новый вариант для разнообразия")
        scored.append(RankedCandidate(candidate, round(score, 6), reasons[:2]))

    scored.sort(key=lambda item: (item.score, -item.candidate.id), reverse=True)

    # Greedy maximal marginal relevance: avoid a page of one category/venue.
    selected: list[RankedCandidate] = []
    remaining = scored[:]
    while remaining and len(selected) < limit:
        def diversified(item: RankedCandidate) -> float:
            category_repeats = sum(
                chosen.candidate.category == item.candidate.category for chosen in selected
            )
            venue_repeats = sum(
                bool(item.candidate.venue_name)
                and chosen.candidate.venue_name == item.candidate.venue_name
                for chosen in selected
            )
            return item.score - 0.11 * category_repeats - 0.06 * venue_repeats

        winner = max(remaining, key=diversified)
        selected.append(winner)
        remaining.remove(winner)
    return selected
