from crosstab_tool.cli import main

VALID = """\
name: cli_test
source: {base_table: s.scores}
scores: [{column: p_support}]
groupings:
  - {column: age, label: Age}
output: {destination: csv, path: out.csv}
"""


def test_validate_ok(tmp_path, capsys):
    p = tmp_path / "job.yaml"
    p.write_text(VALID)
    assert main(["validate", str(p)]) == 0
    assert "avg_p_support" in capsys.readouterr().out


def test_sql_dry_run(tmp_path, capsys):
    p = tmp_path / "job.yaml"
    p.write_text(VALID)
    assert main(["sql", str(p)]) == 0
    out = capsys.readouterr().out
    assert "'01 Age' AS category" in out
    assert out.rstrip().endswith("ORDER BY category, level;")


def test_invalid_config_exits_2(tmp_path, capsys):
    p = tmp_path / "bad.yaml"
    p.write_text("name: x\nsource: {base_table: s.t}\nscores: []\ngroupings: [g]\n")
    assert main(["validate", str(p)]) == 2
    assert "at least one score" in capsys.readouterr().err
