"""Identifier and string-literal safety for dynamically generated SQL
(Req 5.1, security follow-up flagged in the unification plan's Section 3.1).

`query/builder.py` interpolates config-supplied values directly into SQL:
table names, join keys, column names, and score/counterfactual/grouping-
variable names all become part of a generated query string. None of that
value is escaped by a driver-level parameterized query (there's no
placeholder for "the name of a column to group by"), so anything that
reaches these interpolation points has to be validated at generation time
instead. This matters because config files -- unlike code -- may not go
through the same review before landing on a real Redshift connection.

Two categories of interpolated value, two different treatments:

- Identifiers (table/column/alias names): checked against a strict
  allow-list pattern rather than escaped, since there's no valid
  "escaped identifier" for a name that isn't a real column/table/alias in
  the first place. Ported from crosstab-tw's `sqlgen.py` (`_check_identifier`).
- String literals (category/grouping-variable labels, which appear as
  `'<label>' AS category` rather than as identifiers): quoted via SQL's
  standard single-quote-doubling escape, not validated against an
  allow-list, since labels are free text by design (e.g. "00 Topline",
  "18-29"). Ported from crosstab-tw's `_quote_literal`.
"""
from __future__ import annotations

import re

# Optionally schema-qualified identifier: letters/digits/underscores/$,
# starting with a letter or underscore, with up to two `.`-qualifications
# (schema.table or schema.table.column-style refs). Matches crosstab-tw's
# pattern; deliberately does not attempt to support quoted identifiers
# (e.g. "Weird Column") -- if a real column needs quoting, that's a signal
# to rename it, not a case this validator should try to pass through.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$")


class SQLGenerationError(ValueError):
    """Raised when a config value that's about to be interpolated into
    generated SQL doesn't look like a safe identifier or otherwise can't be
    used as given. Distinct from pydantic's schema-level ValidationError:
    this fires at SQL-generation time, against values that were
    individually well-typed (plain strings) but unsafe in combination."""


def check_identifier(name: str, what: str) -> str:
    """Validate `name` as a safe SQL identifier; return it unchanged if so.

    `what` is a short description of the field (e.g. "source.table",
    "join key") used only for the error message.
    """
    if not _IDENTIFIER.match(name):
        raise SQLGenerationError(
            f"{what} {name!r} is not a valid SQL identifier (letters, digits, "
            "underscores, optional up-to-two-level schema qualification -- "
            "e.g. 'schema.table' or 'my_column'). If this is a real "
            "identifier that needs quoting, rename it instead of routing it "
            "through this config."
        )
    return name


def quote_literal(value: str) -> str:
    """Quote `value` as a SQL string literal, escaping embedded single
    quotes by doubling them (the standard SQL escape) -- for free-text
    values like grouping-variable/category labels that are never meant to
    be identifiers."""
    return "'" + str(value).replace("'", "''") + "'"
