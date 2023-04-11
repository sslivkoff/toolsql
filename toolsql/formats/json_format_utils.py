from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import TypeVar
    from typing_extensions import Literal

    import pandas as pd  # type: ignore
    import polars as pl

    T = TypeVar(
        'T',
        typing.Sequence[tuple[typing.Any, ...]],
        pl.DataFrame,
        pd.DataFrame,
    )
    S = TypeVar(
        'S',
        pl.DataFrame,
        pd.DataFrame,
    )

from toolsql import spec


def encode_json_columns(
    *,
    rows: spec.ExecuteManyParams,
    dialect: Literal['sqlite', 'postgresql'],
) -> spec.ExecuteManyParams:

    new_rows: list[typing.Any] | None = None
    for r, row in enumerate(rows):
        new_row: typing.MutableSequence[typing.Any] | typing.MutableMapping[
            str, typing.Any
        ] | None = None
        if isinstance(row, (list, tuple)):
            if any(isinstance(cell, (dict, list, tuple)) for cell in row):
                new_row = [
                    cell
                    if not isinstance(cell, (dict, list, tuple))
                    else encode_json_cell(cell, dialect)
                    for cell in row
                ]
        elif isinstance(row, dict):
            for key, value in row.items():
                if isinstance(value, (dict, list, tuple)):
                    if new_row is None:
                        new_row = row.copy()
                    new_row[key] = encode_json_cell(value, dialect)
        else:
            raise Exception('unknown row')

        if new_row is not None:
            if new_rows is None:
                new_rows = list(rows)
            new_rows[r] = new_row

    if new_rows is not None:
        rows = new_rows

    return rows


def encode_json_cell(item: typing.Any, dialect: spec.Dialect) -> typing.Any:
    if dialect == 'postgresql':
        from psycopg.types.json import Jsonb

        return Jsonb(item)
    elif dialect == 'sqlite':
        import json

        return json.dumps(item)
    else:
        raise Exception('unknown dialect: ' + str(dialect))

