class ProcessingError(ValueError):
    """A configuration cannot be processed under the current text policy."""


class UnsupportedConfigFormatError(ProcessingError):
    """Only text snapshots are supported."""


class ContentUnavailableError(ProcessingError):
    """Referenced content must be resolved by the caller before processing."""


class UnsupportedSecretSyntaxError(ProcessingError):
    """A recognized secret uses unsupported multiline syntax."""


class AssetMismatchError(ProcessingError):
    """A snapshot pair must belong to the same asset."""


class InvalidVersionOrderError(ProcessingError):
    """The current version must be strictly greater than the previous version."""


class SnapshotIdentityError(ProcessingError):
    """Different versions must have different snapshot identifiers."""
