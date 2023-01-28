from __future__ import annotations

import typing

from toolsql import execute_utils
from toolsql import spec
from . import abstract_dbms


class PostgresqlDbms(abstract_dbms.AbstractDbms):
    @classmethod
    def get_tables_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';
        """
        result = execute_utils.select(sql=sql, conn=conn, output_format='tuple')
        return [item[0] for item in result]  # type: ignore

    @classmethod
    def get_indices_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        # raise Exception('get_indices_names() for ' + cls.__name__)
        return []

    @classmethod
    def get_table_schema(
        cls, table_name: str, conn: spec.Connection
    ) -> spec.TableSchema:

        # https://dba.stackexchange.com/a/22368
        sql = """
        SELECT
            column_name as name,
            column_default as default,
            UPPER(data_type) as type,
            is_nullable::boolean as nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """.format(
            table_name=table_name
        )
        raw_columns = execute_utils.select(
            sql=sql, conn=conn, output_format='dict'
        )

        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        primary_keys_sql = """
        SELECT
            a.attname,
            format_type(a.atttypid, a.atttypmod) AS data_type
        FROM
            pg_index i
        JOIN
            pg_attribute a
            ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
        WHERE
            i.indrelid = '{table_name}'::regclass
            AND i.indisprimary""".format(
            table_name=table_name
        )
        raw_pks = execute_utils.select(
            sql=primary_keys_sql, conn=conn, output_format='dict'
        )
        primary_keys = {raw_pk['attname'] for raw_pk in raw_pks}

        for raw_column in raw_columns:
            raw_column['primary'] = raw_column['name'] in primary_keys

            # TODO
            raw_column['index'] = False
            raw_column['unique'] = False

        return {
            'name': table_name,
            'columns': raw_columns,  # type: ignore
            'indices': [],
            'constraints': [],
        }

    @classmethod
    def get_table_create_statement(
        cls, table_name: str, *, conn: spec.Connection
    ) -> str:
        raise Exception('get_table_create_statement() for ' + cls.__name__)

