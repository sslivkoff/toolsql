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
        decode_columns: spec.DecodeColumns | None = None,
        output_format: spec.QueryOutputFormat,
        output_dtypes: spec.OutputDtypes | None = None,
    ) -> spec.SelectOutput:

        if isinstance(conn, str):
            raise Exception('conn not initialized')
        if isinstance(conn, dict):
            raise Exception('conn not initialized')

        cursor = conn.cursor()
        try:
            if parameters is not None:
                cursor = cursor.execute(sql, parameters)
            else:
                cursor = cursor.execute(sql)
        except Exception as e:
            raise spec.convert_exception(e, sql)

        if output_format == 'cursor':
            return cursor

        rows: typing.Sequence[tuple[typing.Any, ...]] = cursor.fetchall()
        rows = formats.decode_columns(rows=rows, columns=decode_columns)

        if output_format == 'tuple':
            return rows
        else:
            names = cls.get_cursor_output_names(cursor)
            return formats.format_row_tuples(
                rows=rows,
                names=names,
                output_format=output_format,
                output_dtypes=output_dtypes,
            )

    @classmethod
    async def _async_select(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.AsyncConnection | spec.DBConfig | str,
        output_format: spec.QueryOutputFormat,
        decode_columns: spec.DecodeColumns | None = None,
        output_dtypes: spec.OutputDtypes | None = None,
    ) -> spec.AsyncSelectOutput:

        if isinstance(conn, str):
            raise Exception('conn not initialized')
        if isinstance(conn, dict):
            raise Exception('conn not initialized')

        try:
            cursor: spec.AsyncCursor = await conn.execute(sql, parameters)
        except Exception as e:
            raise spec.convert_exception(e, sql)
        if output_format == 'cursor':
            return cursor

        rows: typing.Sequence[tuple[typing.Any, ...]] = await cursor.fetchall()
        rows = formats.decode_columns(rows=rows, columns=decode_columns)
        if output_format == 'tuple':
            return rows
        else:
            names = cls.get_cursor_output_names(cursor)
            return formats.format_row_tuples(
                rows=rows,
                names=names,
                output_format=output_format,
                output_dtypes=output_dtypes,
            )

