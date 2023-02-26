from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import TypeVar
    from typing_extensions import Literal

    import pandas as pd
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


def decode_json_columns(
    *,
    rows: T,
    driver: spec.DriverClass,
    columns: typing.Sequence[int] | None = None,
    cursor: spec.Cursor | spec.AsyncCursor | None,
) -> T:

    if isinstance(rows, (tuple, list)):
        if cursor is None:
            raise Exception('must specify cursor to get column names')
        return _decode_json_columns_sequence(
            rows=rows,
            driver=driver,
            columns=columns,
            cursor=cursor,
        )
    elif spec.is_polars_dataframe(rows):
        return _decode_json_columns_polars(  # type: ignore
            rows=rows,
            driver=driver,
            columns=columns,
        )
    elif spec.is_pandas_dataframe(rows):
        return _decode_json_columns_pandas(  # type: ignore
            rows=rows,
            driver=driver,
            columns=columns,
        )
    else:
        raise Exception('invalid rows format: ' + str(type(rows)))


def _decode_json_columns_sequence(
    *,
    rows: typing.Sequence[tuple[typing.Any, ...]],
    driver: spec.DriverClass,
    columns: typing.Sequence[int] | None = None,
    cursor: spec.Cursor | spec.AsyncCursor,
) -> typing.Sequence[tuple[typing.Any, ...]]:

    if columns is not None and driver.name in ['sqlite3', 'aiosqlite']:
        import json

        rows = [
            tuple(
                json.loads(cell) if c in columns else cell
                for c, cell in enumerate(row)
            )
            for row in rows
        ]

    return rows


def _decode_json_columns_pandas(
    *,
    rows: pd.DataFrame,
    driver: spec.DriverClass,
    columns: typing.Sequence[int] | None = None,
) -> pd.DataFrame:

    if columns is not None:
        import json

        for c in columns:
            column_name = rows.columns[c]
            rows[column_name] = rows[column_name].map(json.loads)

    return rows


def _decode_json_columns_polars(
    *,
    rows: pl.DataFrame,
    driver: spec.DriverClass,
    columns: typing.Sequence[int] | None = None,
) -> pl.DataFrame:

    if columns is not None:
        import json
        import polars as pl

        for c in columns:
            column_name = rows.columns[c]
            rows = rows.with_column(
                rows[column_name].apply(json.loads, return_dtype=pl.Object)
            )

    return rows

