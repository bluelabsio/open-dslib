"""Integration tests for the SQL source (M4).

docs/implementation-plan.md calls for validating this against a containerized/local
Postgres via testcontainers. This sandbox has no Docker available, so these tests run
against a local SQLite database instead. ConnectorX (and therefore SQLSource) doesn't
care which backend a URI points at -- these still exercise the real code path (a real
SQL query, executed via ConnectorX, converted to a Polars frame), not a mock. Swapping
`sqlite://...` for `postgresql://...`/`mysql://...`/etc. in a real deployment requires
no code changes; only the connection string differs.
"""

import sqlite3

import pytest

from crosstab_tool import CrosstabSpec, run_crosstab

pytest.importorskip("connectorx", exc_type=ImportError)


@pytest.fixture
def scores_db(tmp_path):
    path = tmp_path / "scores.db"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE scores (id INTEGER, region TEXT, product TEXT, model_score REAL)"
    )
    conn.executemany(
        "INSERT INTO scores VALUES (?, ?, ?, ?)",
        [
            (1, "A", "x", 10.0),
            (2, "A", "y", 20.0),
            (3, "B", "x", 100.0),
            (4, "B", "x", 200.0),
            (5, "B", "y", 300.0),
        ],
    )
    conn.commit()
    conn.close()
    return path


def _spec(db_path, query, groups, stats, filters=None):
    return CrosstabSpec.model_validate(
        {
            "source": {"type": "sql", "connection": f"sqlite://{db_path}", "query": query},
            "score_columns": ["model_score"],
            "groupby": {"type": "explicit", "groups": groups},
            "stats": stats,
            "filters": filters or [],
        }
    )


def test_sql_source_end_to_end(scores_db):
    spec = _spec(
        scores_db,
        "SELECT * FROM scores",
        groups=[["region"]],
        stats=[
            {"name": "count", "column": "model_score"},
            {"name": "mean", "column": "model_score"},
        ],
    )
    result = run_crosstab(spec)

    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [2, 3]
    assert frame["model_score_mean"].to_list() == pytest.approx([15.0, 200.0])


def test_sql_source_query_can_pre_filter(scores_db):
    # Documents the v1 pattern: since there's no GROUP BY pushdown, narrow the query
    # itself for anything expensive rather than relying on `filters`.
    spec = _spec(
        scores_db,
        "SELECT * FROM scores WHERE model_score > 15",
        groups=[["region"]],
        stats=[{"name": "count", "column": "model_score"}],
    )
    result = run_crosstab(spec)

    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [1, 3]


def test_sql_source_multiple_groupsets(scores_db):
    spec = _spec(
        scores_db,
        "SELECT * FROM scores",
        groups=[["region"], ["region", "product"], []],
        stats=[{"name": "count", "column": "model_score"}],
    )
    result = run_crosstab(spec)

    assert set(result.frames) == {"region", "region__product", "__overall__"}
    assert result.frames["__overall__"]["model_score_count"].to_list() == [5]


def test_sql_source_filters_still_apply_client_side(scores_db):
    spec = _spec(
        scores_db,
        "SELECT * FROM scores",
        groups=[["region"]],
        stats=[{"name": "count", "column": "model_score"}],
        filters=["model_score > 15"],
    )
    result = run_crosstab(spec)

    frame = result.frames["region"].sort("region")
    assert frame["region"].to_list() == ["A", "B"]
    assert frame["model_score_count"].to_list() == [1, 3]


def test_sql_source_unknown_column_fails_validation(scores_db):
    spec = _spec(
        scores_db,
        "SELECT * FROM scores",
        groups=[["not_a_real_column"]],
        stats=[{"name": "count", "column": "model_score"}],
    )
    with pytest.raises(ValueError, match="not_a_real_column"):
        run_crosstab(spec)


def test_sql_source_only_queries_once_per_run(scores_db, monkeypatch):
    import polars as pl

    real_read = pl.read_database_uri
    calls = []

    def counting_read(query, connection):
        calls.append(query)
        return real_read(query, connection)

    monkeypatch.setattr(pl, "read_database_uri", counting_read)

    spec = _spec(
        scores_db,
        "SELECT * FROM scores",
        groups=[["region"]],
        stats=[{"name": "count", "column": "model_score"}],
    )
    run_crosstab(spec)

    assert len(calls) == 1
