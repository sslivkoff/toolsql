from __future__ import annotations

import typing

from toolsql import formats
from toolsql import spec

from . import abstract_driver


class DbapiDriver(abstract_driver.AbstractDriver):
    @classmethod
    def _select(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.Connection | spec.DBConfig | str,
        output_format: spec.QueryOutputFormat,
        decode_json_columns: typing.Sequence[int] | None = None,
    ) -> spec.SelectOutput:

        if isinstance(conn, str):
            raise Exception('conn not initialized')
        if isinstance(conn, dict):
            raise Exception('conn not initialized')

        cursor = conn.cursor()
        if parameters is not None:
            cursor = cursor.execute(sql, parameters)
        else:
            cursor = cursor.execute(sql)

        if output_format == 'cursor':
            return cursor

        rows: typing.Sequence[tuple[typing.Any, ...]] = cursor.fetchall()
        rows = formats.decode_json_columns(
            rows=rows,
            driver=cls,
            decode_columns=decode_json_columns,
            cursor=cursor,
        )

        if output_format == 'tuple':
            return rows
        else:
            names = cls.get_cursor_output_names(cursor)
            return formats.format_row_tuples(
                rows=rows, names=names, output_format=output_format
            )

    @classmethod
    async def _async_select(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.AsyncConnection | spec.DBConfig | str,
        output_format: spec.QueryOutputFormat,
        decode_json_columns: typing.Sequence[int] | None = None,
    ) -> spec.AsyncSelectOutput:

        if isinstance(conn, str):
            raise Exception('conn not initialized')
        if isinstance(conn, dict):
            raise Exception('conn not initialized')

        cursor: spec.AsyncCursor = await conn.execute(sql, parameters)
        if output_format == 'cursor':
            return cursor

        rows: typing.Sequence[tuple[typing.Any, ...]] = await cursor.fetchall()
        rows = formats.decode_json_columns(
            rows=rows,
            driver=cls,
            decode_columns=decode_json_columns,
            cursor=cursor,
        )
        if output_format == 'tuple':
            return rows
        else:
            names = cls.get_cursor_output_names(cursor)
            return formats.format_row_tuples(
                rows=rows, names=names, output_format=output_format
            )

