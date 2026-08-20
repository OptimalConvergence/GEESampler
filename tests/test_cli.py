import json

from geesampler.cli import main


def test_catalog_stats_cli_needs_no_earth_engine_authentication(tmp_path, capsys):
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
auth:
  project: placeholder-project
run:
  output_dir: {tmp_path / "runs"}
  monitoring: {{enabled: false}}
catalog:
  enabled: true
  path: {tmp_path / "catalog.sqlite"}
""",
        encoding="utf-8",
    )
    assert main(["catalog", "stats", str(config)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["scenes"] == 0
    assert payload["size_bytes"] > 0
    assert "auth" not in payload
