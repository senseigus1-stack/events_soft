from datetime import datetime, timedelta, timezone

from events_api.recommender import Candidate, HistoryItem, build_preference_weights, rank_candidates


NOW = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)


def candidate(
    event_id: int,
    category: str,
    *,
    days: int = 3,
    popularity: int = 10,
    venue: str = "",
) -> Candidate:
    return Candidate(
        id=event_id,
        category=category,
        tags=(category, ),
        starts_at=NOW + timedelta(days=days),
        popularity=popularity,
        venue_name=venue,
    )


def test_explicit_interest_ranks_matching_event_first():
    ranked = rank_candidates(
        [candidate(1, "Театр"), candidate(2, "Музыка")],
        ["Музыка"],
        [],
        now=NOW,
    )

    assert ranked[0].candidate.id == 2
    assert "музыка" in ranked[0].reasons[0]


def test_negative_action_reduces_feature_weight():
    history = [
        HistoryItem("dismiss", NOW - timedelta(days=1), "Театр", ("Театр",)),
        HistoryItem("save", NOW - timedelta(days=1), "Музыка", ("Музыка",)),
    ]
    weights = build_preference_weights([], history, NOW)

    assert weights["театр"] < 0
    assert weights["музыка"] > 0


def test_old_actions_have_less_influence_than_recent_actions():
    history = [
        HistoryItem("save", NOW - timedelta(days=180), "Кино", ("Кино",)),
        HistoryItem("save", NOW - timedelta(days=2), "Музыка", ("Музыка",)),
    ]
    weights = build_preference_weights([], history, NOW)

    assert weights["музыка"] > weights["кино"] * 10


def test_ranking_diversifies_repeated_categories():
    candidates = [
        candidate(1, "Музыка", popularity=100),
        candidate(2, "Музыка", popularity=95),
        candidate(3, "Театр", popularity=90),
    ]
    ranked = rank_candidates(candidates, [], [], now=NOW, limit=3)

    assert ranked[0].candidate.id == 1
    assert ranked[1].candidate.category == "Театр"


def test_past_events_are_removed():
    past = candidate(1, "Музыка")
    past = Candidate(**{**past.__dict__, "starts_at": NOW - timedelta(days=1)})

    assert rank_candidates([past], [], [], now=NOW) == []


def test_friends_attendance_is_explained_and_boosted():
    social = Candidate(
        **{**candidate(1, "Театр").__dict__, "friend_going_count": 2}
    )
    ranked = rank_candidates(
        [social, candidate(2, "Музыка")], [], [], now=NOW, limit=2
    )

    assert ranked[0].candidate.id == 1
    assert any("друг" in reason for reason in ranked[0].reasons)
