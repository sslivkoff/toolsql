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
        return [item[0] for item in result]

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

        primary_keys = cls._get_table_primary_keys(table_name, conn=conn)
        unique_single_columns, unique_multi_columns = cls._get_unique_columns(
            table_name, conn=conn
        )
        indexed_single_columns, indexed_multi_columns = cls._get_indexed_columns(
            table_name, conn=conn
        )
        for raw_column in raw_columns:
            raw_column['primary'] = raw_column['name'] in primary_keys

            # TODO
            raw_column['index'] = raw_column['name'] in indexed_single_columns
            raw_column['unique'] = raw_column['name'] in unique_single_columns

        return {
            'name': table_name,
            'columns': raw_columns,  # type: ignore
            'indices': [],
            'constraints': [],
        }

    @classmethod
    def _get_table_primary_keys(
        cls, table_name: str, conn: spec.Connection
    ) -> set[str]:
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
        return {raw_pk['attname'] for raw_pk in raw_pks}

    @classmethod
    def _get_unique_columns(
        cls, table_name: str, conn: spec.Connection
    ) -> tuple[set[str], set[set[str]]]:
        # https://stackoverflow.com/a/27752061
        sql = """
        SELECT * 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
            inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu 
                on cu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
        where 
            tc.CONSTRAINT_TYPE = 'UNIQUE'
            and tc.TABLE_NAME = '{table_name}'
        """.format(
            table_name=table_name
        )
        raw_unique = execute_utils.select(
            sql=sql, conn=conn, output_format='dict'
        )

        index_columns: dict[str, list[str]] = {}
        for item in raw_unique:
            index_name = item['constraint_name']
            column_name = item['column_name']
            index_columns.setdefault(index_name, [])
            index_columns[index_name].append(column_name)

        return cls._sort_single_multi_column_indices(index_columns)

    @classmethod
    def _get_indexed_columns(
        cls, table_name: str, conn: spec.Connection
    ) -> tuple[set[str], set[set[str]]]:

        sql = """
        select
            t.relname as table_name,
            i.relname as index_name,
            a.attname as column_name
        from
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        where
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            and t.relname = '{table_name}'
        order by
            t.relname,
            i.relname
        """.format(
            table_name=table_name
        )
        indices = execute_utils.select(
            sql=sql,
            conn=conn,
            output_format='dict',
        )

        index_columns: dict[str, list[str]] = {}
        for item in indices:
            index_name = item['index_name']
            column_name = item['column_name']
            index_columns.setdefault(index_name, [])
            index_columns[index_name].append(column_name)

        return cls._sort_single_multi_column_indices(index_columns)

    @classmethod
    def get_table_create_statement(
        cls, table_name: str, *, conn: spec.Connection
    ) -> str:
        raise Exception('get_table_create_statement() for ' + cls.__name__)

