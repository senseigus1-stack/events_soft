from events_api.config import Settings
from events_api.security import issue_user_token, verify_user_token


def test_signed_user_token_is_bound_to_one_user():
    settings = Settings(_env_file=None, app_secret="a-secure-test-secret")
    token = issue_user_token("user-1", settings)

    assert verify_user_token(token, "user-1", settings)
    assert not verify_user_token(token, "user-2", settings)
    assert not verify_user_token(f"{token}broken", "user-1", settings)
