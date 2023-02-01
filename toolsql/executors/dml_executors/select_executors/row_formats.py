from __future__ import annotations

import typing

from toolsql import spec

if typing.TYPE_CHECKING:
    import polars as pl


def format_row_tuples(
    rows: typing.Sequence[tuple[typing.Any, ...]],
    output_format: spec.QueryOutputFormat | None = None,
    *,
    names: typing.Sequence[str] | None,
) -> spec.RowOutput:

    if output_format == 'tuple':
        return rows

    if names is None:
        if len(rows) == 0:
            output: spec.DictRows = []
            return output
        else:
            raise Exception('could not determine names of columns')

    if output_format == 'dict':
        return [dict(zip(names, row)) for row in rows]
    elif output_format == 'polars':
        import polars as pl

        return pl.DataFrame([tuple(row) for row in rows], columns=names)
    elif output_format == 'pandas':
        import pandas as pd

        # better way to convert rows to dataframes?
        return pd.DataFrame([tuple(row) for row in rows], columns=names)  # type: ignore
    else:
        raise Exception('unknown output format: ' + str(output_format))


def format_row_dataframe(
    rows: pl.DataFrame, output_format: spec.QueryOutputFormat
) -> spec.RowOutput:

    # convert to output_format
    if output_format is None:
        output_format = 'polars'
    if output_format == 'polars':
        import polars as pl

        if isinstance(rows, pl.DataFrame):
            return rows
        else:
            raise Exception('improper format')
    elif output_format == 'pandas':
        import pandas as pd

        if isinstance(rows, pd.DataFrame):
            return rows
        else:
            raise Exception('improper format')
    elif output_format == 'tuple':
        return list(zip(*rows.to_dict().values()))
    elif output_format == 'dict':
        as_dicts: spec.DictRows = rows.to_dicts()
        return as_dicts
    else:
        raise Exception('unknown output format')


def _convert_json_columns(
    *,
    rows: typing.Sequence[tuple[typing.Any, ...]],
    driver: spec.DriverClass,
    raw_column_types: typing.Mapping[str, str] | None = None,
    cursor: spec.Cursor | spec.AsyncCursor,
) -> typing.Sequence[tuple[typing.Any, ...]]:

    if raw_column_types is not None and driver.name in ['sqlite3', 'aiosqlite']:
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

