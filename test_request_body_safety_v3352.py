import ast
from pathlib import Path


def test_all_direct_write_req_calls_pass_json_body_by_keyword():
    """Regression guard for the positional-body bug that broke stop replacement."""
    tree=ast.parse(Path("server.py").read_text(),"server.py")
    bad=[]
    writes=[]
    for node in ast.walk(tree):
        if not isinstance(node,ast.Call) or not isinstance(node.func,ast.Name) or node.func.id!="req":
            continue
        if len(node.args)<2 or not isinstance(node.args[1],ast.Constant):
            continue
        method=node.args[1].value
        if method not in {"POST","PUT","PATCH"}:
            continue
        writes.append(node.lineno)
        keywords={k.arg for k in node.keywords}
        if len(node.args)>3 or "body" not in keywords:
            bad.append(node.lineno)
    assert writes, "Expected at least one direct write call through req()"
    assert bad==[], f"Write req() calls must use body=body, unsafe lines: {bad}"
