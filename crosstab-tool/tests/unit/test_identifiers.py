from __future__ import annotations

import pytest

from crosstab_tool.config.schema import (
    BaseConfig,
    ColumnRef,
    DataSourceConfig,
    GroupingVariable,
    JobConfig,
    JobMeta,
    OutputConfig,
    OutputDestination,
)
from crosstab_tool.query.builder import build_query
from crosstab_tool.query.identifiers import SQLGenerationError, check_identifier, quote_literal


def _minimal_config(**overrides) -> JobConfig:
    defaults = dict(
        job=JobMeta(name="t", model_version="v1"),
        connection="REDSHIFT_MAIN",
        sources=[DataSourceConfig(name="base", table="schema.tbl")],
        base=BaseConfig(**{"from": "base"}),
        scores=[ColumnRef(name="p_support", source="base", column="p_support")],
        grouping_variables=[GroupingVariable(label="01 Age", column="age_bucket")],
        output=OutputConfig(destination=OutputDestination.CSV, path="/tmp/x.csv"),
    )
    defaults.update(overrides)
    return JobConfig(**defaults)


# --- check_identifier / quote_literal unit behavior -------------------------

def test_check_identifier_accepts_schema_qualified_name():
    assert check_identifier("schema.table_name", "test") == "schema.table_name"


@pytest.mark.parametrize(
    "value",
    [
        "tbl; DROP TABLE users; --",
        "tbl -- comment",
        "tbl) UNION SELECT password FROM users --",
        "col\" OR \"1\"=\"1",
        "a.b.c.d",  # more than two qualification levels
        "",
        "1abc",  # can't start with a digit
    ],
)
def test_check_identifier_rejects_injection_shaped_values(value):
    with pytest.raises(SQLGenerationError):
        check_identifier(value, "test field")


def test_quote_literal_escapes_embedded_quotes():
    assert quote_literal("O'Brien") == "'O''Brien'"


# --- build_query end-to-end: malicious config values get rejected ----------

def test_malicious_table_name_rejected():
    config = _minimal_config(
        sources=[
            DataSourceConfig(
                name="base",
                table="tbl; DROP TABLE voterfile; --",
            )
        ]
    )
    with pytest.raises(SQLGenerationError):
        build_query(config)


def test_malicious_join_key_rejected():
    from crosstab_tool.config.schema import JoinConfig

    config = _minimal_config(
        sources=[
            DataSourceConfig(name="base", table="schema.tbl"),
            DataSourceConfig(name="scores", table="schema.scores"),
        ],
        base=BaseConfig(
            **{"from": "base"},
            joins=[JoinConfig(source="scores", key="id) OR (1=1", how="left")],
        ),
    )
    with pytest.raises(SQLGenerationError):
        build_query(config)


def test_malicious_grouping_variable_column_rejected():
    config = _minimal_config(
        grouping_variables=[GroupingVariable(label="01 Age", column="age; DROP TABLE x; --")]
    )
    with pytest.raises(SQLGenerationError):
        build_query(config)


def test_malicious_score_column_rejected():
    config = _minimal_config(
        scores=[ColumnRef(name="p_support", source="base", column="col FROM secrets --")]
    )
    with pytest.raises(SQLGenerationError):
        build_query(config)


# --- legitimate free-text labels are escaped, not rejected ------------------

def test_grouping_label_with_apostrophe_is_escaped_not_rejected():
    config = _minimal_config(
        grouping_variables=[GroupingVariable(label="Voter's Age", column="age_bucket")]
    )
    sql = build_query(config)  # must not raise
    assert "'Voter''s Age' AS category" in sql


def test_raw_query_escape_hatch_is_not_identifier_checked():
    """`query` (vs. `table`) is trusted, hand-written SQL wrapped as a
    derived table -- it should pass through unchanged, not be rejected as a
    malformed identifier."""
    config = _minimal_config(
        sources=[
            DataSourceConfig(
                name="base",
                query="SELECT * FROM schema.tbl WHERE active = true",
            )
        ]
    )
    sql = build_query(config)
    assert "(SELECT * FROM schema.tbl WHERE active = true) AS base" in sql
