import getpass

import polars as pl

from crosstab_tool.sources.sql import SQLSource
from crosstab_tool.spec.source_spec import SQLSourceSpec


def test_missing_connection_prompts_for_credentials_and_builds_uri(monkeypatch):
    prompts = []

    def fake_input(prompt):
        prompts.append(prompt)
        return {"Database host": "warehouse.internal", "Database name": "scores_db", "Username": "alice"}[
            prompt.rstrip(": ")
        ]

    monkeypatch.setattr("builtins.input", fake_input)
    monkeypatch.setattr(getpass, "getpass", lambda prompt="": "s3cr3t!")

    seen_connections = []

    def fake_read_database_uri(query, connection):
        seen_connections.append(connection)
        return pl.DataFrame({"region": ["A"], "score": [1.0]})

    monkeypatch.setattr(pl, "read_database_uri", fake_read_database_uri)

    spec = SQLSourceSpec(query="SELECT * FROM t", dialect="redshift")
    source = SQLSource(spec)

    source.describe_schema()
    source.to_polars_lazyframe().collect()

    assert seen_connections == ["redshift://alice:s3cr3t%21@warehouse.internal:5439/scores_db"]
    assert len(prompts) == 3  # host, database, username -- only prompted once (cached)


def test_fetch_is_cached_across_describe_schema_and_lazyframe(monkeypatch):
    """SQLSource must not re-run `query` for each adapter method call -- only
    core/runner.py building a single adapter instance (see its comment on why)
    protects against re-running it *per run_crosstab() call*; this protects against
    the narrower case of multiple calls against the same instance."""
    calls = []

    def fake_read_database_uri(query, connection):
        calls.append((query, connection))
        return pl.DataFrame({"region": ["A", "B"], "score": [1.0, 2.0]})

    monkeypatch.setattr(pl, "read_database_uri", fake_read_database_uri)

    source = SQLSource(SQLSourceSpec(connection="sqlite:///x.db", query="SELECT * FROM t"))

    schema = source.describe_schema()
    frame = source.to_polars_lazyframe().collect()

    assert set(schema) == {"region", "score"}
    assert frame.shape == (2, 2)
    assert len(calls) == 1
