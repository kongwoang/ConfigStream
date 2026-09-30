import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from generator import main as cli
from generator.engine import WorkloadGenerator
from schemas import ChangeEvent, ConfigSnapshot


def test_cli_is_deterministic_across_processes_and_hash_seeds() -> None:
    command = [
        sys.executable,
        "-m",
        "generator.main",
        "--assets",
        "10",
        "--events",
        "100",
        "--seed",
        "42",
        "--no-sleep",
    ]
    outputs = []
    for hash_seed in ("1", "123"):
        result = subprocess.run(
            command,
            capture_output=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": hash_seed},
        )
        assert result.stderr == b""
        outputs.append(result.stdout)
    assert outputs[0] == outputs[1]
    assert len(outputs[0].splitlines()) == 200


def test_stdout_contains_only_valid_jsonl(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["--assets", "3", "--events", "8", "--seed", "42", "--no-sleep"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    lines = [json.loads(line) for line in captured.out.splitlines()]
    assert len(lines) == 16
    assets: set[str] = set()
    for index in range(0, len(lines), 2):
        assert lines[index]["record_type"] == "change_event"
        assert lines[index + 1]["record_type"] == "config_snapshot"
        event = ChangeEvent.model_validate(lines[index]["payload"])
        snapshot = ConfigSnapshot.model_validate(lines[index + 1]["payload"])
        assert event.asset_id == snapshot.asset_id
        assets.add(event.asset_id)
    assert len(assets) > 1


@pytest.mark.parametrize(
    "duration,rate,count", [("0.3", "10", 3), ("1.25", "2", 2), ("0.01", "1", 0)]
)
def test_duration_means_floor_of_simulated_duration_times_rate(
    duration: str, rate: str, count: int, capsys: pytest.CaptureFixture[str]
) -> None:
    cli.main(["--duration", duration, "--rate", rate, "--no-sleep"])
    assert len(capsys.readouterr().out.splitlines()) == count * 2


def test_duration_and_event_count_have_the_same_output(capsys: pytest.CaptureFixture[str]) -> None:
    cli.main(["--duration", "0.3", "--rate", "10", "--no-sleep"])
    duration_output = capsys.readouterr().out
    cli.main(["--events", "3", "--rate", "10", "--no-sleep"])
    assert capsys.readouterr().out == duration_output


def test_file_output_is_utf8_and_refuses_to_overwrite(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "nested" / "sample.jsonl"
    args = ["--assets", "3", "--events", "8", "--no-sleep", "--output", str(output)]
    assert cli.main(args) == 0
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""
    original = output.read_bytes()
    assert len(original.decode("utf-8").splitlines()) == 16
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2
    assert output.read_bytes() == original
    assert "File exists" in capsys.readouterr().err


def test_no_sleep_mode_does_not_read_or_wait_on_wall_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_clock(*args: object) -> None:
        pytest.fail("wall clock must not be used in no-sleep mode")

    monkeypatch.setattr(cli, "sleep", unexpected_clock)
    monkeypatch.setattr(cli, "monotonic", unexpected_clock)
    output = io.StringIO()
    cli.write_workload(WorkloadGenerator(assets=1, rate=1), 100, output, paced=False)
    assert len(output.getvalue().splitlines()) == 200


def test_pacing_uses_absolute_deadlines_without_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 10.0
    sleeps = []

    def fake_sleep(seconds: float) -> None:
        nonlocal now
        sleeps.append(seconds)
        now += seconds

    class SlowOutput(io.StringIO):
        def write(self, text: str) -> int:
            nonlocal now
            now += 0.01
            return super().write(text)

    monkeypatch.setattr(cli, "monotonic", lambda: now)
    monkeypatch.setattr(cli, "sleep", fake_sleep)
    paced_output = SlowOutput()
    cli.write_workload(WorkloadGenerator(assets=1, rate=10), 3, paced_output)
    assert sleeps == pytest.approx([0.09, 0.09])
    offline_output = io.StringIO()
    cli.write_workload(WorkloadGenerator(assets=1, rate=10), 3, offline_output, paced=False)
    assert offline_output.getvalue() == paced_output.getvalue()


def test_slow_sink_does_not_drop_events(monkeypatch: pytest.MonkeyPatch) -> None:
    times = iter([0.0, 1.0, 2.0, 3.0])
    monkeypatch.setattr(cli, "monotonic", lambda: next(times))
    monkeypatch.setattr(cli, "sleep", lambda _: pytest.fail("must not sleep when behind schedule"))
    output = io.StringIO()
    cli.write_workload(WorkloadGenerator(rate=100), 3, output)
    assert len(output.getvalue().splitlines()) == 6


def test_cli_supports_custom_start_time_and_mix(capsys: pytest.CaptureFixture[str]) -> None:
    cli.main(
        [
            "--assets",
            "2",
            "--events",
            "1",
            "--start-time",
            "2026-10-01T07:00:00+07:00",
            "--asset-mix",
            "nginx_server,generic_service",
            "--no-sleep",
        ]
    )
    record = json.loads(capsys.readouterr().out.splitlines()[0])["payload"]
    assert record["event_time"] == "2026-10-01T00:00:00Z"
    assert record["metadata"]["asset_type"] in {"nginx_server", "generic_service"}


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--events", "1", "--duration", "1"],
        ["--events", "-1"],
        ["--events", "1.5"],
        ["--events", "1", "--assets", "0"],
        ["--events", "1", "--rate", "0"],
        ["--events", "1", "--rate", "1000001"],
        ["--duration", "0"],
        ["--duration", "-1"],
        ["--duration", "NaN"],
        ["--duration", "Infinity"],
        ["--duration", "1e1000000"],
        ["--duration", "invalid"],
        ["--events", "1", "--start-time", "2026-09-30T00:00:00"],
        ["--events", "1", "--start-time", "invalid"],
        ["--events", "1", "--asset-mix", "unknown"],
        ["--events", "1", "--asset-mix", ""],
        ["--events", "1", "--asset-mix", "nginx_server,"],
    ],
)
def test_invalid_cli_arguments_fail_without_output(
    args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as error:
        cli.main([*args, "--no-sleep"])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error:" in captured.err


def test_help_is_available_without_workload_arguments(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        cli.main(["--help"])
    assert error.value.code == 0
    assert "--no-sleep" in capsys.readouterr().out
