from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from events_api.database import Base, get_session
from events_api.config import Settings, get_settings
from events_api.community import generate_due_notifications
from events_api.main import create_app
from events_api.models import City, Event, Notification


def build_client() -> tuple[TestClient, Session]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)
    session.add(City(slug="msk", name="Москва", timezone="Europe/Moscow"))
    session.add_all(
        [
            Event(
                source="test", source_id="music", city_slug="msk", title="Концерт",
                description="Живой звук", category="Музыка", tags=["Музыка"],
                starts_at=datetime.now(timezone.utc) + timedelta(days=2), venue_name="Клуб",
                event_url="https://example.test/1", popularity=100,
            ),
            Event(
                source="test", source_id="theatre", city_slug="msk", title="Спектакль",
                description="Новая сцена", category="Театр", tags=["Театр"],
                starts_at=datetime.now(timezone.utc) + timedelta(days=3), venue_name="Театр",
                event_url="https://example.test/2", popularity=10, is_free=True,
            ),
        ]
    )
    session.commit()
    app = create_app()
    app.dependency_overrides[get_session] = lambda: session
    app.dependency_overrides[get_settings] = lambda: Settings(
        _env_file=None,
        environment="test",
        admin_api_key="admin-test-key",
        sync_api_key="sync-test-key",
    )
    return TestClient(app), session


def test_event_filters_and_details():
    client, session = build_client()
    try:
        response = client.get("/api/v1/events", params={"city": "msk", "free_only": True})
        assert response.status_code == 200
        assert response.json()["total"] == 1
        event_id = response.json()["items"][0]["id"]
        assert client.get(f"/api/v1/events/{event_id}").json()["title"] == "Спектакль"
    finally:
        session.close()


def test_health_cities_and_missing_event():
    client, session = build_client()
    try:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/api/v1/cities").json()[0]["name"] == "Москва"
        assert client.get("/api/v1/events/999").status_code == 404
    finally:
        session.close()


def test_interests_influence_recommendations():
    client, session = build_client()
    try:
        assert client.put("/api/v1/users/u1/interests", json={"tags": ["Театр"]}).status_code == 204
        response = client.get("/api/v1/users/u1/recommendations", params={"city": "msk"})
        assert response.status_code == 200
        assert response.json()["items"][0]["category"] == "Театр"
        assert response.json()["strategy"] == "kytchi-v1"
    finally:
        session.close()


def test_interaction_rejects_unknown_event():
    client, session = build_client()
    try:
        response = client.post("/api/v1/users/u1/interactions", json={"event_id": 999, "action": "save"})
        assert response.status_code == 404
    finally:
        session.close()


def test_interaction_is_recorded_for_existing_event():
    client, session = build_client()
    try:
        event_id = client.get("/api/v1/events").json()["items"][0]["id"]
        response = client.post("/api/v1/users/u1/interactions", json={"event_id": event_id, "action": "save"})
        assert response.status_code == 201
        assert response.json() == {"status": "recorded"}
    finally:
        session.close()


def test_swipe_left_removes_event_from_future_recommendations():
    client, session = build_client()
    try:
        before = client.get("/api/v1/users/u1/recommendations", params={"city": "msk"}).json()
        dismissed_id = before["items"][0]["id"]
        assert client.post(
            "/api/v1/users/u1/interactions",
            json={"event_id": dismissed_id, "action": "dismiss"},
        ).status_code == 201
        after = client.get("/api/v1/users/u1/recommendations", params={"city": "msk"}).json()
        assert dismissed_id not in {item["id"] for item in after["items"]}
    finally:
        session.close()


def test_going_plan_creates_a_durable_reminder():
    client, session = build_client()
    try:
        event_id = client.get("/api/v1/events").json()["items"][0]["id"]
        response = client.put(
            f"/api/v1/users/u1/plans/{event_id}",
            json={"status": "going", "reminder_minutes_before": 10000},
        )
        assert response.status_code == 200
        assert client.get("/api/v1/users/u1/plans").json()[0]["event"]["id"] == event_id
        assert generate_due_notifications(session, datetime.now(timezone.utc)) == 1
        reminder = session.query(Notification).one()
        assert reminder.kind == "event_reminder"
    finally:
        session.close()


def test_friendship_exposes_the_friends_public_plan():
    client, session = build_client()
    try:
        client.put("/api/v1/users/u1/profile", json={"display_name": "Аня", "city_slug": "msk"})
        client.put("/api/v1/users/u2/profile", json={"display_name": "Лев", "city_slug": "msk"})
        friendship = client.post(
            "/api/v1/users/u1/friends", json={"friend_user_id": "u2"}
        ).json()
        assert client.post(f"/api/v1/users/u2/friends/{friendship['id']}/accept").status_code == 200
        event_id = client.get("/api/v1/events").json()["items"][0]["id"]
        client.put(f"/api/v1/users/u2/plans/{event_id}", json={"status": "going"})
        friends = client.get("/api/v1/users/u1/friends").json()
        assert friends[0]["friend"]["display_name"] == "Лев"
        assert friends[0]["shared_events"][0]["id"] == event_id
        recommended = client.get(
            "/api/v1/users/u1/recommendations", params={"city": "msk"}
        ).json()["items"]
        friend_event = next(item for item in recommended if item["id"] == event_id)
        assert any("друг" in reason for reason in friend_event["reasons"])
    finally:
        session.close()


def test_community_event_is_hidden_until_admin_approval():
    client, session = build_client()
    try:
        starts_at = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        response = client.post(
            "/api/v1/users/organizer/event-submissions",
            json={
                "city_slug": "msk",
                "title": "Ночной маркет зинов",
                "description": "Локальные авторы, независимые журналы и разговоры с издателями.",
                "category": "Фестивали",
                "tags": ["Книги", "Маркет"],
                "starts_at": starts_at,
                "venue_name": "Дом печати",
                "address": "Тестовая улица, 1",
                "price_text": "Бесплатно",
                "is_free": True,
            },
        )
        assert response.status_code == 201
        submission = response.json()
        assert submission["status"] == "pending"
        assert client.get(f"/api/v1/events/{submission['id']}").status_code == 404
        moderation = client.post(
            f"/api/v1/admin/event-submissions/{submission['id']}/decision",
            headers={"X-Admin-Key": "admin-test-key"},
            json={"decision": "approved", "note": "Проверено"},
        )
        assert moderation.status_code == 200
        assert moderation.json()["status"] == "published"
        assert client.get(f"/api/v1/events/{submission['id']}").status_code == 200
    finally:
        session.close()


def test_production_user_routes_require_the_signed_session_token():
    client, session = build_client()
    try:
        client.app.dependency_overrides[get_settings] = lambda: Settings(
            _env_file=None,
            environment="production",
            app_secret="production-test-secret",
            admin_api_key="admin-test-key",
            sync_api_key="sync-test-key",
        )
        assert client.get("/api/v1/users/unknown/profile").status_code == 401
        auth = client.post("/api/v1/auth/session").json()
        response = client.get(
            f"/api/v1/users/{auth['user_id']}/profile",
            headers={"Authorization": f"Bearer {auth['token']}"},
        )
        assert response.status_code == 200
        assert response.json()["id"] == auth["user_id"]
    finally:
        session.close()
