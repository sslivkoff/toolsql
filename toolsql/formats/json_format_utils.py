from __future__ import annotations

import typing
from typing_extensions import Literal

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

