from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    """Base for every spec model: rejects unrecognized fields instead of pydantic's
    default of silently dropping them.

    This matters specifically because config files (YAML/JSON) are this tool's
    reproducible, declarative interface (requirement 7) -- a typo'd field name, or a
    field that documents a not-yet-implemented idea (e.g. a `sampling` block sketched
    in docs/POLARS_SCALE.md but not yet built), must fail loudly rather than silently
    doing nothing.
    """

    model_config = ConfigDict(extra="forbid")
