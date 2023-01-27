from __future__ import annotations

import typing

from toolsql import spec
from . import row_formats


def _select_dbapi(
    *,
    sql: str,
    parameters: spec.SqlParameters | None = None,
    conn: spec.Connection,
    output_format: spec.QueryOutputFormat = 'dict',
    driver: spec.DriverClass,
) -> spec.SelectOutput:

    cursor = conn.cursor()
    if parameters is not None:
        cursor = cursor.execute(sql, parameters)
    else:
        cursor = cursor.execute(sql)

    if output_format == 'cursor':
        return cursor

    rows = cursor.fetchall()
    if output_format == 'tuple':
        return rows
    else:
        names = driver.get_cursor_output_names(cursor)
        return row_formats.format_row_tuples(
            rows=rows, names=names, output_format=output_format
        )


async def _async_select_dbapi(
    *,
    sql: str,
    parameters: spec.SqlParameters | None = None,
    conn: spec.AsyncConnection,
    output_format: spec.QueryOutputFormat = 'dict',
    driver: spec.DriverClass,
) -> spec.AsyncSelectOutput:

    cursor: spec.AsyncCursor = await conn.execute(sql, parameters)
    if output_format == 'cursor':
        return cursor

    rows: typing.Sequence[tuple[typing.Any, ...]] = await cursor.fetchall()
    if output_format == 'tuple':
        return rows
    else:
        names = driver.get_cursor_output_names(cursor)
        return row_formats.format_row_tuples(
            rows=rows, names=names, output_format=output_format
        )

