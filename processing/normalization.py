import re

from processing.errors import (
    ContentUnavailableError,
    UnsupportedConfigFormatError,
    UnsupportedSecretSyntaxError,
)
from processing.models import NormalizedConfig
from schemas import ConfigSnapshot

_VOLATILE_COMMENT = re.compile(
    r"^[ \t]*(?:#[ \t]*(?:generated[ \t]+at[ \t]+\S.*|last[ \t]+modified:[ \t]*\S.*)"
    r"|![ \t]*generated(?:[ \t]+.*)?)$",
    re.IGNORECASE,
)
_SECRET_SETTING = re.compile(
    r"^([ \t]*)(password|passwd|secret|token|api_key|api-key)"
    r"([ \t]*[=:][ \t]*|[ \t]+)(.*)$",
    re.IGNORECASE,
)
_SETTING = re.compile(
    r"^([ \t]*)([A-Za-z_][A-Za-z0-9_.-]*)([ \t]*=[ \t]*|[ \t]*:[ \t]+|[ \t]+)(.*)$"
)


def _check_secret_syntax(value: str) -> None:
    unsupported = (
        value.endswith("\\")
        or value.startswith(('"""', "'''"))
        or re.fullmatch(r"[|>][0-9+-]*(?:[ \t]+#.*)?", value) is not None
    )
    if not unsupported and value.startswith(('"', "'")):
        quote = value[0]
        escaped = False
        for character in value[1:]:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                break
        else:
            unsupported = True
    if unsupported:
        raise UnsupportedSecretSyntaxError("Multiline secret values are not supported")


def _normalize_line(line: str) -> str:
    secret = _SECRET_SETTING.fullmatch(line)
    setting = secret or _SETTING.fullmatch(line)
    if setting is None:
        return line
    indentation, name, separator, value = setting.groups()
    if secret is not None:
        _check_secret_syntax(value)
        value = "***"
    separator = "=" if "=" in separator else ": " if ":" in separator else " "
    return f"{indentation}{name}{separator}{value}"


def normalize_text(content: str) -> str:
    """Canonicalize ordered, line-oriented text; recognized secrets lose their RHS."""
    if not isinstance(content, str):
        raise TypeError("Configuration content must be a string")
    lines: list[str] = []
    for raw_line in content.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw_line.rstrip(" \t")
        if _VOLATILE_COMMENT.fullmatch(line):
            continue
        if not line:
            if lines and lines[-1]:
                lines.append("")
            continue
        lines.append(_normalize_line(line))
    if lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines) + "\n" if lines else ""


def normalize_snapshot(snapshot: ConfigSnapshot) -> NormalizedConfig:
    """Normalize validated inline text without mutating or fetching source content."""
    identity = (
        f"snapshot {snapshot.snapshot_id!r}, asset {snapshot.asset_id!r}, "
        f"version {snapshot.version}"
    )
    if snapshot.config_format != "text":
        raise UnsupportedConfigFormatError(f"Only text configuration is supported: {identity}")
    if snapshot.content is None:
        raise ContentUnavailableError(f"Inline content is required: {identity}")
    try:
        content = normalize_text(snapshot.content)
    except UnsupportedSecretSyntaxError:
        raise UnsupportedSecretSyntaxError(
            f"Multiline secret values are not supported: {identity}"
        ) from None
    return NormalizedConfig(
        asset_id=snapshot.asset_id,
        snapshot_id=snapshot.snapshot_id,
        version=snapshot.version,
        normalized_content=content,
    )
