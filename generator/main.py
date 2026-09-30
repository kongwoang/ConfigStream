import argparse
import sys
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal, DecimalException, InvalidOperation
from pathlib import Path
from time import monotonic, sleep
from typing import TextIO

from generator.engine import DEFAULT_START_TIME, MAX_RATE, WorkloadGenerator
from generator.serialization import serialize_change
from generator.templates import DEFAULT_ASSET_MIX, TEMPLATES


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def nonnegative_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a nonnegative integer")
    return parsed


def parse_duration(value: str) -> Decimal:
    try:
        duration = Decimal(value)
    except InvalidOperation as error:
        raise argparse.ArgumentTypeError("duration must be a positive finite number") from error
    if not duration.is_finite() or duration <= 0:
        raise argparse.ArgumentTypeError("duration must be a positive finite number")
    return duration


def parse_start_time(value: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("start-time must be an ISO 8601 timestamp") from error
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise argparse.ArgumentTypeError("start-time must include a timezone")
    return timestamp.astimezone(UTC)


def parse_asset_mix(value: str) -> tuple[str, ...]:
    asset_mix = tuple(asset_type.strip() for asset_type in value.split(","))
    if any(asset_type not in TEMPLATES for asset_type in asset_mix):
        raise argparse.ArgumentTypeError(f"asset-mix supports: {', '.join(TEMPLATES)}")
    return asset_mix


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate stateful configuration changes as JSONL."
    )
    parser.add_argument("--assets", type=positive_integer, default=100)
    count = parser.add_mutually_exclusive_group(required=True)
    count.add_argument("--events", type=nonnegative_integer, help="number of configuration changes")
    count.add_argument(
        "--duration", type=parse_duration, help="simulated seconds; floor(duration * rate) changes"
    )
    parser.add_argument(
        "--rate",
        type=positive_integer,
        default=100,
        help=f"changes/sec, 1..{MAX_RATE} (default: 100)",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--start-time",
        type=parse_start_time,
        default=DEFAULT_START_TIME,
        help="timezone-aware ISO 8601 (default: 2026-09-30T00:00:00Z)",
    )
    parser.add_argument(
        "--asset-mix",
        type=parse_asset_mix,
        default=DEFAULT_ASSET_MIX,
        help="comma-separated types assigned cyclically; repetitions weight the mix",
    )
    parser.add_argument(
        "--output", default="-", help="new UTF-8 JSONL file, or - for stdout (default)"
    )
    parser.add_argument(
        "--no-sleep",
        action="store_true",
        help="disable wall-clock pacing, not simulated event time",
    )
    return parser


def write_workload(
    generator: WorkloadGenerator,
    events: int,
    output: TextIO,
    paced: bool = True,
) -> None:
    started = monotonic() if paced else 0.0
    for index, (event, snapshot) in enumerate(generator.generate(events)):
        if paced:
            remaining = started + index / generator.rate - monotonic()
            if remaining > 0:
                sleep(remaining)
        output.write(serialize_change(event, snapshot))
        if paced:
            output.flush()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        generator = WorkloadGenerator(
            assets=args.assets,
            seed=args.seed,
            rate=args.rate,
            start_time=args.start_time,
            asset_mix=args.asset_mix,
        )
        events = args.events if args.events is not None else int(args.duration * args.rate)
        if args.output == "-":
            destination = nullcontext(sys.stdout)
        else:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            destination = path.open("x", encoding="utf-8", newline="\n")
        with destination as output:
            write_workload(generator, events, output, paced=not args.no_sleep)
    except (OSError, ValueError, OverflowError, DecimalException) as error:
        parser.exit(2, f"{parser.prog}: error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
