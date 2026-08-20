from geesampler.models import EECUMonitorConfig
from geesampler.monitoring import EECUMonitor, EECUSnapshot, SequenceEECUReader


def test_eecu_thresholds_and_metrics(tmp_path):
    reader = SequenceEECUReader(
        [EECUSnapshot("2026-01-01T00:00:00Z", completed_seconds=3600, in_progress_seconds=20)]
    )
    monitor = EECUMonitor(
        EECUMonitorConfig(
            enabled=True,
            poll_seconds=1,
            warning_eecu_hours=0.5,
            hard_eecu_hours=1,
            price_per_eecu_hour=0.4,
        ),
        reader,
        tmp_path / "eecu.jsonl",
        lambda: 4,
    )
    snapshot = monitor.poll_once()
    assert snapshot.completed_seconds == 3600
    assert monitor.warning_reached
    assert monitor.hard_limit_reached
    text = (tmp_path / "eecu.jsonl").read_text(encoding="utf-8")
    assert '"eecu_per_success": 900.0' in text
    assert '"projected_cost": 0.4' in text


def test_optional_missing_reader_does_not_stop(tmp_path):
    monitor = EECUMonitor(
        EECUMonitorConfig(enabled=True, required=False),
        None,
        tmp_path / "eecu.jsonl",
        lambda: 0,
    )
    monitor.start()
    assert not monitor.hard_limit_reached
    assert monitor.unavailable_error


def test_in_progress_usage_can_trigger_guard(tmp_path):
    reader = SequenceEECUReader(
        [EECUSnapshot("2026-01-01T00:00:00Z", completed_seconds=20, in_progress_seconds=3600)]
    )
    monitor = EECUMonitor(
        EECUMonitorConfig(enabled=True, hard_eecu_hours=1),
        reader,
        tmp_path / "eecu.jsonl",
        lambda: 0,
    )
    monitor.poll_once()
    assert monitor.hard_limit_reached
