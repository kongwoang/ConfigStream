from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ConfigTemplate:
    prefix: str
    base: dict[str, str]
    scalar_values: dict[str, tuple[str, ...]]
    features: tuple[str, ...]
    optional_settings: dict[str, tuple[str, ...]]


TEMPLATES = {
    "network_device": ConfigTemplate(
        prefix="router",
        base={
            "hostname": "{asset_id}",
            "logging": "enabled",
            "ssh": "enabled",
            "telnet": "disabled",
            "dns": "8.8.8.8",
            "interface eth0": "enabled",
            "route 0.0.0.0/0": "via 10.0.0.1",
        },
        scalar_values={"dns": ("8.8.8.8", "1.1.1.1", "9.9.9.9")},
        features=("logging", "ssh", "telnet", "interface eth0"),
        optional_settings={
            "route 10.10.0.0/16": ("via 10.0.0.2", "via 10.0.0.3"),
            "interface eth0 description": ("primary uplink", "backup uplink"),
            "user operator": ("role read-only", "role admin"),
        },
    ),
    "nginx_server": ConfigTemplate(
        prefix="nginx",
        base={
            "server_name": "{asset_id}",
            "worker_processes": "2",
            "access_log": "enabled",
            "error_log": "enabled",
            "keepalive_timeout": "65",
            "gzip": "enabled",
        },
        scalar_values={
            "worker_processes": ("1", "2", "4", "8"),
            "keepalive_timeout": ("30", "65", "90", "120"),
        },
        features=("access_log", "error_log", "gzip"),
        optional_settings={
            "client_max_body_size": ("8m", "16m", "32m"),
            "worker_connections": ("512", "1024", "2048"),
        },
    ),
    "generic_service": ConfigTemplate(
        prefix="service",
        base={
            "service_name": "{asset_id}",
            "logging": "enabled",
            "debug": "disabled",
            "timeout": "30",
            "replicas": "2",
        },
        scalar_values={"timeout": ("15", "30", "60", "120"), "replicas": ("1", "2", "3", "5")},
        features=("logging", "debug"),
        optional_settings={"retry_limit": ("2", "3", "5"), "cache_ttl": ("60", "120", "300")},
    ),
}

DEFAULT_ASSET_MIX = tuple(TEMPLATES)


def initial_config(asset_type: str, asset_id: str) -> dict[str, str]:
    return {
        setting: value.format(asset_id=asset_id)
        for setting, value in TEMPLATES[asset_type].base.items()
    }


def render_config(config: dict[str, str]) -> str:
    return "".join(f"{setting} {value}\n" for setting, value in config.items())
