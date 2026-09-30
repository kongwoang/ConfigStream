from random import Random

import pytest

from generator.mutations import mutate_config
from generator.templates import DEFAULT_ASSET_MIX, TEMPLATES, initial_config, render_config

MUTATIONS = ("change_scalar", "enable_feature", "disable_feature", "add_setting", "remove_setting")


@pytest.mark.parametrize("asset_type", DEFAULT_ASSET_MIX)
@pytest.mark.parametrize("mutation_name", MUTATIONS)
def test_meaningful_mutations_do_not_modify_input(asset_type: str, mutation_name: str) -> None:
    config = initial_config(asset_type, "asset-001")
    rng = Random(42)
    if mutation_name == "remove_setting":
        config = mutate_config(asset_type, config, rng, "add_setting").config
    if mutation_name == "enable_feature":
        config = mutate_config(asset_type, config, rng, "disable_feature").config
    previous = config.copy()
    result = mutate_config(asset_type, config, rng, mutation_name)

    assert config == previous
    assert result.name == mutation_name
    assert result.config != previous
    assert render_config(result.config) != render_config(previous)
    assert result.old_value == previous.get(result.setting)
    assert result.new_value == result.config.get(result.setting)
    assert result.old_value != result.new_value
    assert set(TEMPLATES[asset_type].base) <= set(result.config)
    changed = {
        setting
        for setting in previous.keys() | result.config.keys()
        if previous.get(setting) != result.config.get(setting)
    }
    assert changed == {result.setting}


@pytest.mark.parametrize("asset_type", DEFAULT_ASSET_MIX)
def test_long_mutation_history_is_bounded_and_valid(asset_type: str) -> None:
    config = initial_config(asset_type, "asset-001")
    template = TEMPLATES[asset_type]
    rng = Random(123)
    seen: set[str] = set()
    for _ in range(500):
        result = mutate_config(asset_type, config, rng)
        assert result.config != config
        config = result.config
        seen.add(result.name)
        assert (
            set(template.base)
            <= config.keys()
            <= template.base.keys() | template.optional_settings.keys()
        )
        assert all(config[setting] in {"enabled", "disabled"} for setting in template.features)
        for setting, values in (template.scalar_values | template.optional_settings).items():
            if setting in config:
                assert config[setting] in values
    assert seen == set(MUTATIONS)


def test_unavailable_mutation_is_rejected_instead_of_becoming_noop() -> None:
    config = initial_config("generic_service", "service-001")
    with pytest.raises(ValueError, match="not applicable"):
        mutate_config("generic_service", config, Random(42), "remove_setting")
    with pytest.raises(ValueError, match="not applicable"):
        mutate_config("generic_service", config, Random(42), "unknown")


def test_exhausted_optional_settings_select_other_mutations() -> None:
    config = initial_config("generic_service", "service-001")
    rng = Random(42)
    for _ in TEMPLATES["generic_service"].optional_settings:
        config = mutate_config("generic_service", config, rng, "add_setting").config
    with pytest.raises(ValueError, match="not applicable"):
        mutate_config("generic_service", config, rng, "add_setting")
    result = mutate_config("generic_service", config, rng)
    assert result.name != "add_setting"
    assert result.config != config


def test_templates_have_independent_identity_and_configuration() -> None:
    first = initial_config("network_device", "router-000001")
    second = initial_config("network_device", "router-000002")
    first["logging"] = "disabled"
    assert second["logging"] == "enabled"
    assert second["hostname"] == "router-000002"
    assert render_config(second).endswith("\n")
