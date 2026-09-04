from pathlib import Path

p = Path("autonomous_asset_optimizer.py")
text = p.read_text(encoding="utf-8")
block = '''def integrity_artifact_failed(report:Mapping[str,Any]):
 return str(report.get("status") or "UNKNOWN").upper()!="PASS" or bool(report.get("failures") or [])

def integrity_artifact_failed(report:Mapping[str,Any]):
 return str(report.get("status") or "UNKNOWN").upper()!="PASS" or bool(report.get("failures") or [])
'''
replacement = '''def integrity_artifact_failed(report:Mapping[str,Any]):
 return str(report.get("status") or "UNKNOWN").upper()!="PASS" or bool(report.get("failures") or [])
'''
if block not in text:
    raise SystemExit("duplicate helper block not found")
p.write_text(text.replace(block, replacement, 1), encoding="utf-8")
