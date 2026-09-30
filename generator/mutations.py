from dataclasses import dataclass
from random import Random

from generator.templates import TEMPLATES


@dataclass(frozen=True, slots=True)
class MutationResult:
    config: dict[str, str]
    name: str
    setting: str
    old_value: str | None
    new_value: str | None


def mutate_config(
    asset_type: str,
    current_config: dict[str, str],
    rng: Random,
    mutation_name: str | None = None,
) -> MutationResult:
    template = TEMPLATES[asset_type]
    candidates: dict[str, list[tuple[str, str | None]]] = {
        "change_scalar": [
            (setting, value)
            for setting, values in (template.scalar_values | template.optional_settings).items()
            if setting in current_config
            for value in values
            if value != current_config[setting]
        ],
        "enable_feature": [
            (setting, "enabled")
            for setting in template.features
            if current_config.get(setting) == "disabled"
        ],
        "disable_feature": [
            (setting, "disabled")
            for setting in template.features
            if current_config.get(setting) == "enabled"
        ],
        "add_setting": [
            (setting, value)
            for setting, values in template.optional_settings.items()
            if setting not in current_config
            for value in values
        ],
        "remove_setting": [
            (setting, None) for setting in template.optional_settings if setting in current_config
        ],
    }
    available = [name for name, options in candidates.items() if options]
    if mutation_name is None:
        if not available:
            raise ValueError("configuration has no applicable mutations")
        mutation_name = rng.choice(available)
    elif mutation_name not in available:
        raise ValueError(f"mutation {mutation_name!r} is not applicable to this configuration")

    setting, new_value = rng.choice(candidates[mutation_name])
    updated = current_config.copy()
    old_value = updated.get(setting)
    if new_value is None:
        del updated[setting]
    else:
        updated[setting] = new_value
    return MutationResult(updated, mutation_name, setting, old_value, new_value)
