from __future__ import annotations

import typing

from toolsql import executors
from toolsql import spec
from toolsql import statements
from . import abstract_db


class SqliteDb(abstract_db.AbstractDb):

    @classmethod
    def create_db(cls, db_config: spec.DBConfig) -> None:
        import sqlite3

        path = db_config['path']
        if path is None:
            raise Exception('path not specified for db')
        with sqlite3.connect(path):
            pass

    @classmethod
    def get_tables_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        sql = """SELECT name FROM sqlite_schema WHERE type =='table'"""
        result = executors.raw_select(
            sql=sql,
            conn=conn,
            output_format='tuple',
        )
        return [item[0] for item in result]

    @classmethod
    def get_table_raw_column_types(
        cls,
        table: str | spec.TableSchema,
        conn: spec.Connection | str | spec.DBConfig,
    ) -> typing.Mapping[str, str]:
        sql = 'SELECT name, type FROM pragma_table_info("{table_name}")'.format(
            table_name=statements.get_table_name(table)
        )
        result = executors.raw_select(sql=sql, conn=conn, output_format='tuple')
        return dict(result)  # type: ignore

    @classmethod
    async def async_get_table_raw_column_types(
        cls,
        table: str | spec.TableSchema,
        conn: spec.AsyncConnection | str | spec.DBConfig,
    ) -> typing.Mapping[str, str]:
        sql = 'SELECT name, type FROM pragma_table_info("{table_name}")'.format(
            table_name=statements.get_table_name(table)
        )
        result = await executors.async_raw_select(sql=sql, conn=conn, output_format='tuple')
        return dict(result)  # type: ignore

    @classmethod
    def get_table_schema(
        cls, table: str | spec.TableSchema, conn: spec.Connection | str | spec.DBConfig
    ) -> spec.TableSchema:

        table_name = statements.get_table_name(table)

        if isinstance(conn, str):
            raise Exception('conn not initialized')
        if isinstance(conn, dict):
            raise Exception('conn not initialized')

        sql = 'SELECT * FROM pragma_table_info("{table_name}")'.format(table_name=table_name)
        cursor = conn.execute(sql)
        results = cursor.fetchall()
        unique_single_columns, unique_multi_columns = cls._get_unique_columns(
            table_name, conn=conn
        )
        (
            indexed_single_columns,
            indexed_multi_columns,
        ) = cls._get_indexed_columns(table_name, conn=conn)

        columns: list[spec.ColumnSchema] = []
        for cid, name, type, not_null, default, primary in results:

            if primary == 0:
                primary = False
            elif primary == 1:
                primary = True
            else:
                raise Exception('unknown primary format')

            column: spec.ColumnSchema = {
                'name': name,
                'type': type,
                'nullable': not not_null,
                'default': default,
                'primary': primary,
                'index': name in indexed_single_columns,
                'unique': name in unique_single_columns,
            }
            columns.append(column)

        if len(unique_multi_columns) > 0 or len(indexed_multi_columns) > 0:
            raise NotImplementedError('multicolumn_indices')

        return {
            'name': table_name,
            'columns': columns,
            'indices': [],
            'constraints': [],
        }

    @classmethod
    def _get_unique_columns(
        cls, table_name: str, conn: spec.Connection
    ) -> tuple[set[str], set[set[str]]]:

        # https://stackoverflow.com/a/65845786
        unique_sql = """
        SELECT
          m.tbl_name AS table_name,
          il.name AS key_name,
          ii.name AS column_name
        FROM
          sqlite_master AS m,
          pragma_index_list(m.name) AS il,
          pragma_index_info(il.name) AS ii
        WHERE
          m.type = 'table' AND
          m.tbl_name = '{table_name}' AND
          il.origin = 'u'
        ORDER BY table_name, key_name, ii.seqno
        """
        unique_sql = unique_sql.format(table_name=table_name)
        cursor = conn.execute(unique_sql)
        unique_results = cursor.fetchall()
        index_columns: typing.MutableMapping[str, list[str]] = {}
        for _, index_name, column_name in unique_results:
            index_columns.setdefault(index_name, [])
            index_columns[index_name].append(column_name)

        return cls._sort_single_multi_column_indices(index_columns)

    @classmethod
    def _get_indexed_columns(
        cls, table_name: str, conn: spec.Connection
    ) -> tuple[set[str], set[set[str]]]:

        # get index names
        sql = """
        SELECT name FROM sqlite_schema
        WHERE type == 'index' AND tbl_name == '{table_name}'
        """
        sql = sql.format(table_name=table_name)
        cursor = conn.execute(sql)
        result = cursor.fetchall()
        index_names = [item[0] for item in result]

        # get index info
        column_indices = set()
        multicolumn_indices: set[set[str]] = set()
        for index_name in index_names:
            sql = 'PRAGMA index_info({index_name})'.format(
                index_name=index_name
            )
            cursor = conn.execute(sql)
            results = cursor.fetchall()
            for index_rank, cid, column_name in results:
                if cid == -2:
                    raise NotImplementedError('indexed expression')
            if len(results) == 1:
                column_indices.add(results[0][2])
            else:
                multicolumn_indices.add({result[2] for result in results})

        return column_indices, multicolumn_indices

