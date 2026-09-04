import subprocess

from research_audit import audit_diff_package, orchestrate_tests


def test_test_orchestrator_never_marks_unexecuted_required_sets_pass(tmp_path):
    result=orchestrate_tests(
        ".",new_test_commands=[["python","-c","raise SystemExit(0)"]],
        regression_commands=[["python","-c","raise SystemExit(0)"]],run_full_regression=False,
    )
    assert result["status"]=="PASS"
    assert result["new_module_tests"][0]["executed"] is True
    assert result["full_regression"]["status"]=="NOT TESTED"


def test_package_auditor_detects_prohibited_artifact(tmp_path):
    subprocess.run(["git","init","-q"],cwd=tmp_path,check=True)
    subprocess.run(["git","config","user.email","test@example.com"],cwd=tmp_path,check=True)
    subprocess.run(["git","config","user.name","Test"],cwd=tmp_path,check=True)
    (tmp_path/"safe.py").write_text("x=1\n",encoding="utf-8")
    subprocess.run(["git","add","safe.py"],cwd=tmp_path,check=True)
    subprocess.run(["git","commit","-qm","base"],cwd=tmp_path,check=True)
    base=subprocess.run(["git","rev-parse","HEAD"],cwd=tmp_path,check=True,text=True,capture_output=True).stdout.strip()
    (tmp_path/"result.zip").write_text("bad",encoding="utf-8")
    out=audit_diff_package(tmp_path,base_commit=base)
    assert out["status"]=="FAIL"
    assert "WORKTREE_CONTAMINATED_BY_TRANSIENTS" in out["failures"]
