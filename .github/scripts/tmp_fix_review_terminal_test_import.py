from pathlib import Path

p = Path('test_automation_v3_modes.py')
text = p.read_text(encoding='utf-8')
if '    load_json,\n' not in text:
    text = text.replace('    KEEP_INCUMBENT,\n', '    KEEP_INCUMBENT,\n    load_json,\n', 1)
p.write_text(text, encoding='utf-8')
