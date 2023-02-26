from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import TypeVar

    import pandas as pd
    import polars as pl

    R = TypeVar(
        'R',
        typing.Sequence[tuple[typing.Any, ...]],
        pl.DataFrame,
        pd.DataFrame,
    )

from toolsql import spec
from . import json_format_utils


def decode_columns(
    *,
    rows: R,
    driver: spec.DriverClass,
    columns: spec.DecodeColumns | None = None,
    cursor: spec.Cursor | spec.AsyncCursor | None,
) -> R:

    if columns is None:
        return rows

    json_columns = [c for c, column in enumerate(columns) if column == 'JSON']
    if len(json_columns) > 0:
        rows = json_format_utils.decode_json_columns(
            rows=rows,
            driver=driver,
            columns=json_columns,
            cursor=cursor,
        )

    return rows

