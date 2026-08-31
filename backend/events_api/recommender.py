"""Kytchi AI: adaptive, explainable and privacy-first event ranking.

The model learns a compact preference vector from explicit interests and event
actions. It never assigns demographic, personality or social-status labels and
does not send user history to a third-party model.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import exp, log1p, sqrt, tanh


ACTION_WEIGHT = {
    "open": 0.18,
    "like": 0.85,
    "save": 1.2,
    "attend": 1.8,
    "dismiss": -1.4,
}

ACTION_SIGNAL_VALUE = {
    "open": 0.25,
    "like": 0.8,
    "save": 1.0,
    "attend": 1.35,
    "dismiss": 1.0,
}

TIME_SLOT_LABELS = {
    "morning": "утренний ритм",
    "day": "дневной ритм",
    "evening": "вечерний ритм",
    "night": "ночной ритм",
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
        return {
            feature
            for feature in (
                self.category.strip().casefold(),
                *(tag.strip().casefold() for tag in self.tags),
            )
            if feature
        }


@dataclass(frozen=True)
class HistoryItem:
    action: str
    occurred_at: datetime
    category: str
    tags: tuple[str, ...]
    venue_name: str = ""
    starts_at: datetime | None = None
    is_free: bool = False


@dataclass(frozen=True)
class PreferenceProfile:
    feature_weights: dict[str, float]
    venue_weights: dict[str, float]
    time_weights: dict[str, float]
    free_weight: float
    signal_count: int
    confidence: float
    stage: str
    summary: list[str]


@dataclass
class RankedCandidate:
    candidate: Candidate
    score: float
    match_percent: int
    reasons: list[str] = field(default_factory=list)
    score_components: dict[str, float] = field(default_factory=dict)


def _ensure_aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _time_slot(value: datetime) -> str:
    hour = _ensure_aware(value).hour
    if hour < 11:
        return "morning"
    if hour < 17:
        return "day"
    if hour < 23:
        return "evening"
    return "night"


def _normalized_features(category: str, tags: tuple[str, ...]) -> set[str]:
    return {
        feature
        for feature in (
            category.strip().casefold(),
            *(tag.strip().casefold() for tag in tags),
        )
        if feature
    }


def build_preference_weights(
    explicit_interests: list[str],
    history: list[HistoryItem],
    now: datetime,
) -> dict[str, float]:
    """Build a decayed content vector; retained as the public primitive."""
    weights: defaultdict[str, float] = defaultdict(float)
    for interest in explicit_interests:
        normalized = interest.strip().casefold()
        if normalized:
            weights[normalized] += 1.4

    aware_now = _ensure_aware(now)
    for item in history:
        age_days = max(
            0.0,
            (aware_now - _ensure_aware(item.occurred_at)).total_seconds() / 86400,
        )
        decay = exp(-age_days / 60.0)
        value = ACTION_WEIGHT.get(item.action, 0.0) * decay
        for feature in _normalized_features(item.category, item.tags):
            weights[feature] += value
    return dict(weights)


def build_preference_profile(
    explicit_interests: list[str],
    history: list[HistoryItem],
    now: datetime,
) -> PreferenceProfile:
    """Infer content and context preferences with confidence calibration."""
    aware_now = _ensure_aware(now)
    feature_weights = build_preference_weights(explicit_interests, history, aware_now)
    venue_weights: defaultdict[str, float] = defaultdict(float)
    time_weights: defaultdict[str, float] = defaultdict(float)
    free_weight = 0.0
    effective_signals = float(len({item.strip().casefold() for item in explicit_interests if item.strip()}))

    for item in history:
        age_days = max(
            0.0,
            (aware_now - _ensure_aware(item.occurred_at)).total_seconds() / 86400,
        )
        decay = exp(-age_days / 60.0)
        action_weight = ACTION_WEIGHT.get(item.action, 0.0) * decay
        effective_signals += ACTION_SIGNAL_VALUE.get(item.action, 0.0) * decay
        venue = item.venue_name.strip().casefold()
        if venue:
            venue_weights[venue] += action_weight * 0.7
        if item.starts_at is not None:
            time_weights[_time_slot(item.starts_at)] += action_weight * 0.72
        free_weight += action_weight * (1.0 if item.is_free else -0.22)

    signal_count = len(explicit_interests) + len(history)
    confidence = round(1.0 - exp(-effective_signals / 8.0), 4)
    if effective_signals < 3:
        stage = "exploring"
    elif effective_signals < 8:
        stage = "learning"
    else:
        stage = "personalized"

    summary: list[str] = []
    positive_features = sorted(
        ((feature, weight) for feature, weight in feature_weights.items() if weight > 0.25),
        key=lambda item: item[1],
        reverse=True,
    )
    summary.extend(feature for feature, _ in positive_features[:3])
    if free_weight > 0.75:
        summary.append("бесплатные события")
    positive_time = max(time_weights.items(), key=lambda item: item[1], default=("", 0.0))
    if positive_time[1] > 0.5:
        summary.append(TIME_SLOT_LABELS[positive_time[0]])

    return PreferenceProfile(
        feature_weights=feature_weights,
        venue_weights=dict(venue_weights),
        time_weights=dict(time_weights),
        free_weight=free_weight,
        signal_count=signal_count,
        confidence=confidence,
        stage=stage,
        summary=summary[:4],
    )


def _clamp(value: float, minimum: float = -1.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def rank_candidates(
    candidates: list[Candidate],
    explicit_interests: list[str],
    history: list[HistoryItem],
    *,
    now: datetime | None = None,
    limit: int = 12,
) -> list[RankedCandidate]:
    """Rank with an adaptive preference model and calibrated exploration."""
    now = _ensure_aware(now or datetime.now(timezone.utc))
    profile = build_preference_profile(explicit_interests, history, now)
    max_popularity = max((item.popularity for item in candidates), default=1) or 1
    scored: list[RankedCandidate] = []

    for candidate in candidates:
        hours_until = (_ensure_aware(candidate.starts_at) - now).total_seconds() / 3600
        if hours_until < -2:
            continue

        matches = sorted(
            (
                (feature, profile.feature_weights.get(feature, 0.0))
                for feature in candidate.features
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        positive_matches = [(feature, weight) for feature, weight in matches if weight > 0.18]
        raw_affinity = sum(weight for _, weight in matches) / sqrt(max(1, len(candidate.features)))
        affinity = tanh(raw_affinity / 2.2)

        venue_key = candidate.venue_name.strip().casefold()
        venue_affinity = tanh(profile.venue_weights.get(venue_key, 0.0) / 2.0)
        time_slot = _time_slot(candidate.starts_at)
        time_affinity = tanh(profile.time_weights.get(time_slot, 0.0) / 2.0)
        price_affinity = tanh(profile.free_weight / 2.5) * (1.0 if candidate.is_free else -0.22)
        popularity = log1p(max(0, candidate.popularity)) / log1p(max_popularity)
        social = min(1.0, log1p(candidate.friend_going_count) / log1p(4))

        days_until = max(0.0, hours_until / 24)
        timing = exp(-abs(days_until - 5.0) / 24.0)
        known_features = sum(feature in profile.feature_weights for feature in candidate.features)
        novelty = 1.0 - known_features / max(1, len(candidate.features))
        exploration = (1.0 - profile.confidence) * novelty * 0.18
        community = 0.05 if candidate.source == "community" else 0.0
        freshness = 0.0
        if candidate.created_at is not None:
            age_days = max(
                0.0,
                (now - _ensure_aware(candidate.created_at)).total_seconds() / 86400,
            )
            freshness = 0.05 * exp(-age_days / 21.0)

        components = {
            "taste": round(affinity, 4),
            "context": round((venue_affinity + time_affinity + price_affinity) / 3.0, 4),
            "social": round(social, 4),
            "timing": round(timing, 4),
            "discovery": round(exploration, 4),
        }
        raw_score = (
            0.54 * affinity
            + 0.08 * venue_affinity
            + 0.08 * time_affinity
            + 0.06 * price_affinity
            + 0.13 * social
            + 0.08 * popularity
            + 0.08 * timing
            + exploration
            + community
            + freshness
        )
        probability = 1.0 / (1.0 + exp(-2.35 * (raw_score - 0.05)))
        match_percent = round(38 + probability * 61)

        reasons: list[str] = []
        if positive_matches:
            reasons.append(f"Совпадает с вашим интересом «{positive_matches[0][0]}»")
        if time_affinity > 0.2:
            reasons.append(f"Подходит под ваш {TIME_SLOT_LABELS[time_slot]}")
        if candidate.is_free and price_affinity > 0.18:
            reasons.append("Вы часто выбираете бесплатные события")
        if venue_affinity > 0.2 and candidate.venue_name:
            reasons.append(f"Вам нравятся события в «{candidate.venue_name}»")
        if candidate.friend_going_count:
            word = "друг" if candidate.friend_going_count == 1 else "друга"
            reasons.append(f"Собираются {candidate.friend_going_count} {word}")
        if candidate.popularity and popularity >= 0.72:
            reasons.append("Сейчас набирает внимание")
        if days_until <= 2:
            reasons.append("Можно пойти уже скоро")
        if not reasons:
            reasons.append("Кытчи проверяет новый для вас сценарий")

        scored.append(
            RankedCandidate(
                candidate=candidate,
                score=round(_clamp(probability, 0.0, 1.0), 6),
                match_percent=match_percent,
                reasons=reasons[:3],
                score_components=components,
            )
        )

    scored.sort(key=lambda item: (item.score, -item.candidate.id), reverse=True)

    # Maximal marginal relevance prevents a high-confidence filter bubble.
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
            diversity_strength = 0.09 + 0.05 * profile.confidence
            return (
                item.score
                - diversity_strength * category_repeats
                - 0.055 * venue_repeats
            )

        winner = max(remaining, key=diversified)
        selected.append(winner)
        remaining.remove(winner)
    return selected
