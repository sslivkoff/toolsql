from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import formats
from toolsql import spec


def _select_dbapi(
    *,
    sql: str,
    parameters: spec.ExecuteParams | None = None,
    conn: spec.Connection,
    output_format: spec.QueryOutputFormat = 'dict',
    driver: spec.DriverReference,
    raw_column_types: typing.Mapping[str, str] | None = None,
) -> spec.SelectOutput:

    cursor = conn.cursor()
    if parameters is not None:
        cursor = cursor.execute(sql, parameters)
    else:
        cursor = cursor.execute(sql)

    if output_format == 'cursor':
        return cursor

    rows: typing.Sequence[tuple[typing.Any, ...]] = cursor.fetchall()

    driver = drivers.get_driver_class(driver)

    rows = formats.decode_json_columns(
        rows=rows,
        driver=driver,
        raw_column_types=raw_column_types,
        cursor=cursor,
    )

    if output_format == 'tuple':
        return rows
    else:
        names = driver.get_cursor_output_names(cursor)
        return formats.format_row_tuples(
            rows=rows, names=names, output_format=output_format
        )


async def _async_select_dbapi(
    *,
    sql: str,
    parameters: spec.ExecuteParams | None = None,
    conn: spec.AsyncConnection,
    output_format: spec.QueryOutputFormat = 'dict',
    driver: spec.DriverReference,
    raw_column_types: typing.Mapping[str, str] | None = None,
) -> spec.AsyncSelectOutput:

    cursor: spec.AsyncCursor = await conn.execute(sql, parameters)
    if output_format == 'cursor':
        return cursor

    driver = drivers.get_driver_class(driver)
    rows: typing.Sequence[tuple[typing.Any, ...]] = await cursor.fetchall()
    rows = formats.decode_json_columns(
        rows=rows,
        driver=driver,
        raw_column_types=raw_column_types,
        cursor=cursor,
    )
    if output_format == 'tuple':
        return rows
    else:
        names = driver.get_cursor_output_names(cursor)
        return formats.format_row_tuples(
            rows=rows, names=names, output_format=output_format
        )

