from events_api.config import Settings


def test_comma_separated_environment_values_are_supported():
    settings = Settings(
        _env_file=None,
        cors_origins="https://events.example,https://admin.example",
        sync_city_slugs="msk,spb,ekb",
        timepad_organization_ids="1040, 2040",
    )

    assert settings.cors_origins == ["https://events.example", "https://admin.example"]
    assert settings.sync_city_slugs == ["msk", "spb", "ekb"]
    assert settings.timepad_organization_ids == [1040, 2040]
