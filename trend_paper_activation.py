"""Validate and freeze qualifying pairs from complete 60-day PAPER evidence."""
import hashlib
import json
from pathlib import Path

from major_trend import TREND_VERSION, directional_targets, utc

ACTIVE_FILE = Path(__file__).with_name("ACTIVE_TREND_60D_PAPER.json")


def qualified_definitions(report):
    if (report.get("protocol") != "ONE_60_DAY_CAUSAL_H1_H4_ADAPTIVE_PAPER"
            or report.get("production_authority") is not False
            or report.get("split") is not None or report.get("comparison_periods") != []
            or report.get("trend_version") != TREND_VERSION):
        raise ValueError("Invalid 60-day PAPER evidence protocol")
    window = report["window"]
    if (utc(window["end"]) - utc(window["start"])).total_seconds() != 60 * 86400:
        raise ValueError("Evidence window must be exactly 60 days")
    if not report.get("execution_model", {}).get("closed_within_window_only"):
        raise ValueError("Evidence must contain only closes within the frozen window")
    definitions = {}
    for pair in report["pairs"]:
        targets = directional_targets(pair["exposure_minutes"])
        if not pair.get("pair_qualified"):
            continue
        results = pair["results"]
        if len(results) != 2 or {r["direction"] for r in results} != {"BUY", "SELL"}:
            raise ValueError("A complete BUY and SELL pair is required")
        for row in results:
            direction = row["direction"]
            metrics = row["metrics"]
            if (row["instrument"] != pair["instrument"] or row["verdict"] != "PAPER_CANDIDATE"
                    or metrics["resolved"] != metrics["wins"] + metrics["losses"]
                    or metrics["wins"] <= metrics["losses"] or metrics["expectancy_r"] <= 0
                    or row["minimum_by_regime"] != targets[direction]
                    or any(row["resolved_by_regime"].get(regime, 0) < n for regime, n in targets[direction].items())
                    or not all(row["final_gates"].values())):
                raise ValueError("Candidate failed recomputed evidence gates")
            candidate = dict(row["candidate"])
            sha = candidate.pop("definition_sha256")
            if hashlib.sha256(json.dumps(candidate, sort_keys=True).encode()).hexdigest() != sha:
                raise ValueError("Candidate definition checksum mismatch")
            if (candidate["instrument"] != pair["instrument"] or candidate["direction"] != direction
                    or candidate["trend_version"] != TREND_VERSION or candidate["window"] != window
                    or candidate["minimum_by_regime"] != targets[direction]
                    or candidate["paper_only"] is not True or candidate["production_authority"] is not False):
                raise ValueError("Candidate metadata does not match its evidence")
            definitions[(pair["instrument"], direction)] = candidate
    return definitions


def load_active_definitions(path=ACTIVE_FILE):
    if not Path(path).exists():
        return {}
    return qualified_definitions(json.loads(Path(path).read_text()))


def activate_report(source, destination=ACTIVE_FILE):
    report = json.loads(Path(source).read_text())
    definitions = qualified_definitions(report)
    if not definitions:
        raise ValueError("No pair qualifies; no activation file was written")
    Path(destination).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return len(definitions)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence")
    args = parser.parse_args()
    print(f"Qualified PAPER strategies: {activate_report(args.evidence)}")
