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
    rows: spec.ExecuteManyParams | None = None,
    dialect: Literal['sqlite', 'postgresql'],
) -> spec.ExecuteManyParams | None:
    if rows is not None:
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
    raw_column_types: typing.Mapping[str, str] | None = None,
    cursor: spec.Cursor | spec.AsyncCursor | None,
) -> T:

    if isinstance(rows, (tuple, list)):
        if cursor is None:
            raise Exception('must specify cursor to get column names')
        return _decode_json_columns_sequence(
            rows=rows,
            driver=driver,
            raw_column_types=raw_column_types,
            cursor=cursor,
        )
    elif spec.is_polars_dataframe(rows):
        return _decode_json_columns_polars(  # type: ignore
            rows=rows,
            driver=driver,
            raw_column_types=raw_column_types,
        )
    elif spec.is_pandas_dataframe(rows):
        return _decode_json_columns_pandas(  # type: ignore
            rows=rows,
            driver=driver,
            raw_column_types=raw_column_types,
        )
    else:
        raise Exception('invalid rows format: ' + str(type(rows)))


def _decode_json_columns_sequence(
    *,
    rows: typing.Sequence[tuple[typing.Any, ...]],
    driver: spec.DriverClass,
    raw_column_types: typing.Mapping[str, str] | None = None,
    cursor: spec.Cursor | spec.AsyncCursor,
) -> typing.Sequence[tuple[typing.Any, ...]]:

    if raw_column_types is not None and driver.name in ['sqlite3', 'aiosqlite']:

        # determine which columns are json
        if isinstance(raw_column_types, dict):
            column_names = driver.get_cursor_output_names(cursor)
            if column_names is None:
                raise Exception('could not determine column names of output')
            json_indices = [
                column_names.index(column_name)
                for column_name, columntype in raw_column_types.items()
                if columntype == 'JSON'
            ]
        elif isinstance(raw_column_types, (list, tuple)):
            json_indices = [
                c
                for c, columntype in enumerate(raw_column_types)
                if columntype == 'JSON'
            ]
        else:
            raise Exception('invalid raw_column_types format')

        # convert from json str to python objects
        if len(json_indices) > 0:
            import json

            rows = [
                tuple(
                    json.loads(cell) if c in json_indices else cell
                    for c, cell in enumerate(row)
                )
                for row in rows
            ]

    return rows


def _decode_json_columns_pandas(
    *,
    rows: pd.DataFrame,
    driver: spec.DriverClass,
    raw_column_types: typing.Mapping[str, str] | None = None,
) -> pd.DataFrame:

    if raw_column_types is None:
        return rows

    import json

    for column_name, column_type in raw_column_types.items():
        if column_type in ['JSON', 'JSONB']:
            rows[column_name] = rows[column_name].map(json.loads)
    return rows


def _decode_json_columns_polars(
    *,
    rows: pl.DataFrame,
    driver: spec.DriverClass,
    raw_column_types: typing.Mapping[str, str] | None = None,
) -> pl.DataFrame:
    if raw_column_types is None:
        return rows

    import json
    import polars as pl

    for column_name, column_type in raw_column_types.items():
        if column_type in ['JSON', 'JSONB']:
            rows = rows.with_column(
                rows[column_name].apply(json.loads, return_dtype=pl.Object)
            )
    return rows

