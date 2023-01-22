from __future__ import annotations

import types
from toolsql import spec


class AbstractDriver:
    name: str

    def __init__(self) -> None:
        name = self.__class__.__name__
        raise Exception('do not instantiate ' + name + ', just use its methods')

    @classmethod
    def get_module(cls) -> types.ModuleType:
        import importlib

        return importlib.import_module(cls.name)

    @classmethod
    def select(
        cls,
        conn: spec.Connection,
        sql: str,
        parameters: spec.SqlParameters,
        output_format: spec.QueryOutputFormat | None = None,
    ) -> spec.SelectOutput:

        if isinstance(conn, str):
            raise Exception('conn not initialized')
        cursor = conn.cursor()
        cursor = cursor.execute(sql, parameters)
        if output_format == 'cursor':
            return cursor

        rows = cursor.fetchall()
        if output_format == 'tuple':
            return rows
        elif output_format == 'dict':
            names = cls.get_cursor_output_names(cursor)
            if names is None:
                if len(rows) == 0:
                    output: spec.DictRows = []
                    return output
                else:
                    raise Exception('could not determine names of columns')
            return [dict(zip(names, row)) for row in rows]
        elif output_format in ('polars', 'pandas'):
            names = cls.get_cursor_output_names(cursor)
            if output_format == 'polars':
                import polars as pl

                return pl.DataFrame(rows, columns=names)
            elif output_format == 'pandas':
                import pandas as pd

                return pd.DataFrame([tuple(row) for row in rows], columns=names)  # type: ignore

        raise Exception('unknown output format: ' + str(output_format))

    @classmethod
    async def async_select(
        cls,
        conn: spec.AsyncConnection,
        sql: str,
        parameters: spec.SqlParameters,
        output_format: spec.QueryOutputFormat | None = None,
    ) -> spec.AsyncSelectOutput:

        if isinstance(conn, str):
            raise Exception('conn not initialized')
        cursor = await conn.execute(sql, parameters)
        if output_format == 'cursor':
            return cursor

        rows = await cursor.fetchall()
        if output_format == 'tuple':
            return rows
        elif output_format == 'dict':
            names = cls.get_cursor_output_names(cursor)
            if names is None:
                if len(rows) == 0:
                    output: spec.DictRows = []
                    return output
                else:
                    raise Exception('could not determine names of columns')
            return [dict(zip(names, row)) for row in rows]
        elif output_format in ('polars', 'pandas'):
            names = cls.get_cursor_output_names(cursor)
            if output_format == 'polars':
                import polars as pl

                return pl.DataFrame([tuple(row) for row in rows], columns=names)
            elif output_format == 'pandas':
                import pandas as pd

                # better way to convert rows to dataframes?
                return pd.DataFrame([tuple(row) for row in rows], columns=names)  # type: ignore

        raise Exception('unknown output format: ' + str(output_format))

    @classmethod
    def build_select_query(
        self,
        raw_sql: str,
        raw_parameters: spec.SqlParameters | None,
    ) -> tuple[str, spec.SqlParameters]:
        if raw_parameters is None:
            raw_parameters = tuple()
        return raw_sql, raw_parameters

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor,
    ) -> tuple[str, ...] | None:
        raise NotImplementedError('get_cursor_output_names() for ' + cls.name)

    #
    # # connections
    #

    @classmethod
    def connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.Connection:
        raise NotImplementedError('connect() for ' + cls.name)

    @classmethod
    def async_connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.AsyncConnection:
        raise NotImplementedError('async_connect() for ' + cls.name)

