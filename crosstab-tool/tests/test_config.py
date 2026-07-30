import pytest

from crosstab_tool.config import (
    ConfigError,
    Grouping,
    Job,
    Metric,
    Output,
    Source,
    job_from_dict,
    load_job,
)


def minimal_dict(**overrides):
    d = {
        "name": "test_job",
        "source": {"base_table": "schema.scores"},
        "scores": [{"column": "p_support"}],
        "groupings": [{"column": "age_bucket", "label": "Age"}],
        "output": {"destination": "csv", "path": "out.csv"},
    }
    d.update(overrides)
    return d


def test_minimal_config_valid():
    job = job_from_dict(minimal_dict())
    assert job.groupings[0].category() == "01 Age"
    assert job.result_columns() == ["count", "avg_p_support"]


def test_groupings_accept_bare_strings():
    job = job_from_dict(minimal_dict(groupings=["age_bucket", "party"]))
    assert [g.category() for g in job.groupings] == ["01 age_bucket", "02 party"]


def test_missing_scores_errors():
    with pytest.raises(ConfigError, match="at least one score"):
        job_from_dict(minimal_dict(scores=[]))


def test_missing_source_errors():
    d = minimal_dict()
    del d["source"]
    with pytest.raises(ConfigError, match="source"):
        job_from_dict(d)


def test_unknown_key_errors_with_valid_keys_listed():
    with pytest.raises(ConfigError, match="Unknown key.*basetable"):
        job_from_dict(minimal_dict(scores=[{"column": "p", "basetable": "x"}]))


def test_unknown_aggregation_errors():
    with pytest.raises(ConfigError, match="Unknown aggregation 'kurtosis'"):
        job_from_dict(minimal_dict(aggregations=["kurtosis"]))


def test_bad_join_type_errors():
    d = minimal_dict()
    d["source"]["joins"] = [{"table": "s.t", "key": "id", "how": "cross"}]
    with pytest.raises(ConfigError, match="join type"):
        job_from_dict(d)


def test_categorical_requires_values():
    with pytest.raises(ConfigError, match="values"):
        job_from_dict(
            minimal_dict(counterfactuals=[{"column": "flag", "type": "categorical"}])
        )


def test_duplicate_grouping_order_errors():
    with pytest.raises(ConfigError, match="orders must be unique"):
        job_from_dict(
            minimal_dict(
                groupings=[
                    {"column": "a", "order": 1},
                    {"column": "b", "order": 1},
                ]
            )
        )


def test_cross_column_unknown_column_errors():
    with pytest.raises(ConfigError, match="not produced by this job"):
        job_from_dict(
            minimal_dict(
                cross_column=[{"op": "difference", "left": "avg_p_support", "right": "avg_nope"}]
            )
        )


def test_cross_column_can_chain():
    job = job_from_dict(
        minimal_dict(
            scores=[{"column": "a"}, {"column": "b"}],
            cross_column=[
                {"op": "difference", "left": "avg_a", "right": "avg_b", "label": "gap"},
                {"op": "product", "left": "gap", "right": "avg_a"},
            ],
        )
    )
    assert job.cross_column[1].output_column == "product_gap_avg_a"


def test_output_validation():
    with pytest.raises(ConfigError, match="requires a `path:`"):
        Output(destination="csv")
    with pytest.raises(ConfigError, match="spreadsheet"):
        Output(destination="sheets")


def test_load_job_sets_config_hash(tmp_path):
    p = tmp_path / "job.yaml"
    p.write_text(
        "name: t\n"
        "source: {base_table: s.t}\n"
        "scores: [{column: p}]\n"
        "groupings: [g]\n"
        "output: {destination: csv, path: o.csv}\n"
    )
    job = load_job(str(p))
    assert len(job.config_hash) == 64


def test_python_api_construction():
    job = Job(
        name="api_job",
        source=Source(base_table="s.scores"),
        scores=[Metric(column="p_support")],
        groupings=[Grouping(column="party")],
    )
    assert job.result_columns() == ["count", "avg_p_support"]
