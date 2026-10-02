"""Pure configuration processing, independent of transports and Spark state."""

from processing.diff import compare_snapshots, process_snapshot_pair
from processing.models import DiffResult, NormalizedConfig
from processing.normalization import normalize_snapshot, normalize_text

__all__ = [
    "DiffResult",
    "NormalizedConfig",
    "compare_snapshots",
    "normalize_snapshot",
    "normalize_text",
    "process_snapshot_pair",
]
