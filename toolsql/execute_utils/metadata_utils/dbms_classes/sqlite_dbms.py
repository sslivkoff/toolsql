from __future__ import annotations

import typing

from toolsql import execute_utils
from toolsql import spec
from . import abstract_dbms


class SqliteDbms(abstract_dbms.AbstractDbms):
    @classmethod
    def get_tables_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        sql = """SELECT name FROM sqlite_schema WHERE type =='table'"""
        result = execute_utils.select(sql=sql, conn=conn, output_format='tuple')
        return [item[0] for item in result]  # type: ignore

    @classmethod
    def get_table_schema(
        cls, table_name: str, conn: spec.Connection
    ) -> spec.TableSchema:

        if isinstance(conn, str):
            raise Exception('conn not initialized')

        # column_indices, multicolumn_indices = cls._get_table_indices(
        #     table_name=table_name, conn=conn
        # )

        sql = 'PRAGMA table_info({table_name})'.format(table_name=table_name)
        cursor = conn.execute(sql)
        results = cursor.fetchall()

        columns: list[spec.ColumnSchema] = []
        for cid, name, type, not_null, default, primary in results:
            column: spec.ColumnSchema = {
                'name': name,
                'type': type,
                'nullable': not not_null,
                'default': default,
                'primary': primary,
                #
                # todo
                # 'index': name in column_indices,
                'index': False,
                'unique': False,
            }
            columns.append(column)

        # if len(multicolumn_indices) > 0:
        #     raise NotImplementedError('multicolumn_indices')

        return {
            'name': table_name,
            'columns': columns,
            'indices': [],
            'constraints': [],
        }

    @classmethod
    def _get_table_indices(
        cls, table_name: str, conn: spec.Connection
    ) -> tuple[set[str], typing.Sequence[set[str]]]:

        column_indices: set[str] = set()
        multicolumn_indices: typing.Sequence[set[str]] = []
        return (column_indices, multicolumn_indices)

        # if isinstance(conn, str):
        #     raise Exception('conn not initialized')

        # # get index names
        # sql = """
        # SELECT name FROM sqlite_schema
        # WHERE type == 'index' AND table_name == '{table_name}'
        # """
        # sql = sql.format(table_name=table_name)
        # cursor = conn.execute(sql)
        # result = cursor.fetchall()
        # index_names = [item[0] for item in result]

        # # get index info
        # column_indices = set()
        # multicolumn_indices: list[set[str]] = list()
        # for index_name in index_names:
        #     sql = 'PRAGMA index_info({index_name})'.format(
        #         index_name=index_name
        #     )
        #     cursor = conn.execute(sql)
        #     results = cursor.fetchall()
        #     for index_rank, cid, column_name in results:
        #         if cid == -2:
        #             raise NotImplementedError('indexed expression')
        #     if len(results) == 1:
        #         column_indices.add(results[0][2])
        #     else:
        #         multicolumn_indices.append({result[2] for result in results})

        # return column_indices, multicolumn_indices

    @classmethod
    def get_table_create_statement(
        cls, table_name: str, *, conn: spec.Connection
    ) -> str:
        sql = """
        SELECT sql FROM sqlite_schema
        WHERE type='table' AND table_name = '{table_name}'
        """

        if isinstance(conn, str):
            raise Exception('conn not initialized')

        sql = sql.format(table_name=table_name)
        cursor = conn.execute(sql)
        result = cursor.fetchall()
        if len(result) == 1:
            output = result[0][0]
            if isinstance(output, str):
                return output
            else:
                raise Exception('wrong output type')
        elif len(result) == 0:
            raise Exception('table not found')
        else:
            raise Exception('too many results found')

    @classmethod
    def get_index_create_statement(
        cls, index_name: str, table_name: str, *, conn: spec.Connection
    ) -> str:
        """roll into get_table_create_statement()"""

        raise NotImplementedError()

        # sql = """
        # SELECT sql FROM sqlite_schema
        # WHERE
        #     type='index'
        #     AND table_name = '{table_name}'
        #     AND index_name = '{name}'
        # """
        # sql = sql.format(table_name=table_name, index_name=index_name)
        # cursor = conn.execute(sql)
        # result = cursor.fetchall()
        # if len(result) == 1:
        #     return result[0][0]
        # elif len(result) == 0:
        #     raise Exception('table not found')
        # else:
        #     raise Exception('too many results found')

