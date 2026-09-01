from pathlib import Path

from asset_cycle_manager import cycle_status
from cascade_optimizer import CASCADE
from research_asset import build_stages


def test_master_builds_complete_ordered_offline_cascade(tmp_path):
    repo=Path(__file__).resolve().parent
    cache=tmp_path/"cache.json";cache.write_text("{}",encoding="utf-8")
    workspace=tmp_path/"run";workspace.mkdir();state=workspace/"state.json"
    stages=build_stages(
        repo=repo,python="python",instrument="AUD_USD",cache=cache,workspace=workspace,
        start="2026-01-01T00:00:00Z",end="2026-02-01T00:00:00Z",warmup=10,horizon=240,
        variant="V331_BASELINE",embargo=30,discovery_fraction=.6,validation_fraction=.2,
        min_resolved=10,code_sha="abc",state=state,
    )
    assert tuple(stage.name for stage in stages)==CASCADE
    rendered=" ".join(part for stage in stages for part in stage.command).lower()
    assert "railway" not in rendered and "deployment" not in rendered


def test_asset_cycle_never_marks_asset_permanently_complete():
    out=cycle_status({"assets":{"GBP_USD":{"lifecycle":{"next_allowed_stage":"HUMAN_IA1_REVIEW"}}}})
    assert all(row["permanently_complete"] is False for row in out["rotation"])
    assert out["repeat_with_recent_window"] is True
