from pathlib import Path

p = Path("_tmp_v3_ledger_fix.py")
text = p.read_text(encoding="utf-8")
old = '''# Focused secret/data check over the new diff.\ndiff = subprocess.check_output(["git", "diff", BASE_SHA, "--", "."], cwd=ROOT, text=True)\nfor needle in ("OANDA_TOKEN=", "GH_TOKEN=", "RAILWAY_TOKEN=", "PRIVATE KEY-----"):\n    if needle in diff:\n        raise SystemExit(f"secret-like material found in diff: {needle}")\n\n# Remove temporary validation machinery from the final tree.\nfor path in (ROOT / "_tmp_v3_ledger_fix.py", ROOT / ".github/workflows/tmp-v3-ledger-lookback-fix.yml"):\n    if path.exists():\n        path.unlink()\n'''
new = '''# Remove temporary validation machinery before inspecting the governed diff.\nfor path in (\n    ROOT / "_tmp_v3_ledger_fix.py",\n    ROOT / "_tmp_v3_ledger_fix2.py",\n    ROOT / "_tmp_v3_ledger_trigger.txt",\n    ROOT / ".github/workflows/tmp-v3-ledger-lookback-fix.yml",\n):\n    if path.exists():\n        path.unlink()\n\n# Focused secret/data check over the final governed diff only.\ndiff = subprocess.check_output(["git", "diff", BASE_SHA, "--", "."], cwd=ROOT, text=True)\nfor needle in ("OANDA_TOKEN=", "GH_TOKEN=", "RAILWAY_TOKEN=", "PRIVATE KEY-----"):\n    if needle in diff:\n        raise SystemExit(f"secret-like material found in governed diff: {needle}")\n'''
if old not in text:
    raise SystemExit("secret/cleanup anchor not found")
text = text.replace(old, new, 1)
old = '''run(["git", "add", "automation_v3_modes.py", "test_automation_v3_review_ledger_lookback.py", "_tmp_v3_ledger_fix.py", ".github/workflows/tmp-v3-ledger-lookback-fix.yml"])'''
new = '''run(["git", "add", "-A", "automation_v3_modes.py", "test_automation_v3_review_ledger_lookback.py", "_tmp_v3_ledger_fix.py", "_tmp_v3_ledger_fix2.py", "_tmp_v3_ledger_trigger.txt", ".github/workflows/tmp-v3-ledger-lookback-fix.yml"])'''
if old not in text:
    raise SystemExit("git add anchor not found")
p.write_text(text.replace(old, new, 1), encoding="utf-8")
