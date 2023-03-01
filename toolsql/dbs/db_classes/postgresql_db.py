from __future__ import annotations

import typing

from toolsql import executors
from toolsql import spec
from toolsql import statements
from . import abstract_db


class PostgresqlDb(abstract_db.AbstractDb):

    @classmethod
    def create_db(cls, db_config: spec.DBConfig) -> None:
        import subprocess

        template = (
            'createdb'
            ' --owner={owner}'
            ' --host={hostname}'
            ' --port={port}'
            ' --username={username}'
            ' --password={password}'
            ' {database}'
        )
        cmd = template.format(**db_config)
        subprocess.call(cmd)

    @classmethod
    def get_tables_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';
        """
        result = executors.raw_select(sql=sql, conn=conn, output_format='tuple')
        return [item[0] for item in result]

    @classmethod
    def get_indices_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        # raise Exception('get_indices_names() for ' + cls.__name__)
        return []

    @classmethod
    def get_table_schema(
        cls,
        table: str | spec.TableSchema,
        conn: spec.Connection | str | spec.DBConfig,
    ) -> spec.TableSchema:

        # if isinstance(conn, str):
        #     raise Exception('conn not initialized')
        # if isinstance(conn, dict):
        #     raise Exception('conn not initialized')

        table_name = statements.get_table_name(table)

        # https://dba.stackexchange.com/a/22368
        sql = """
        SELECT
            column_name::TEXT as name,
            column_default as default,
            UPPER(data_type) as type,
            is_nullable::boolean as nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """.format(
            table_name=table_name
        )
        raw_columns = executors.raw_select(
            sql=sql, conn=conn, output_format='dict'
        )

        primary_keys = cls._get_table_primary_keys(table_name, conn=conn)
        unique_single_columns, unique_multi_columns = cls._get_unique_columns(
            table_name, conn=conn
        )
        (
            indexed_single_columns,
            indexed_multi_columns,
        ) = cls._get_indexed_columns(table_name, conn=conn)
        for raw_column in raw_columns:
            raw_column['primary'] = raw_column['name'] in primary_keys

            # TODO
            raw_column['index'] = raw_column['name'] in indexed_single_columns
            raw_column['unique'] = raw_column['name'] in unique_single_columns

            # TODO: support more datatypes for default values
            if raw_column['default'] is not None:
                default = raw_column['default']
                if default[0] == "'" and default[-1] == "'":
                    raw_column['default'] = default.strip("'")
                elif default.endswith('::text'):
                    raw_column['default'] = default.split('::text')[0].strip("'")
                else:
                    raw_column['default'] = int(default)

        # multicolumn indices
        sql = """
        SELECT indexname::TEXT,indexdef::TEXT
        FROM pg_indexes
        WHERE tablename = '{table_name}'
        """.format(table_name=table_name)
        all_indices = executors.raw_select(
            sql=sql, conn=conn, output_format='dict'
        )
        indices = []
        for index in all_indices:
            sql = index['indexdef']
            if '_pkey ' in sql:
                continue
            targets = sql[sql.index('(') + 1:sql.rindex(')')]
            if ',' not in targets:
                continue

            index_columns = []
            for item in targets.split('), '):
                if item == '':
                    continue
                elif statements.is_column_name(item):
                    index_columns.append(item)
                elif item.startswith('COALESCE('):
                    column = item.split('COALESCE(')[1].split(',')[0]
                    index_columns.append(column)
                else:
                    raise Exception('could not parse index expression')

            index_schema: spec.IndexSchema = {
                'name': index['indexname'],
                'columns': index_columns,
                'unique': index['indexdef'].startswith('CREATE UNIQUE'),
                'nulls_equal': 'COALESCE(' in targets,
            }
            indices.append(index_schema)

        return {
            'name': table_name,
            'columns': raw_columns,  # type: ignore
            'indices': indices,
            'constraints': [],
        }

    @classmethod
    async def async_get_table_schema(
        cls,
        table: str | spec.TableSchema,
        conn: spec.AsyncConnection | str | spec.DBConfig,
    ) -> spec.TableSchema:

        # if isinstance(conn, str):
        #     raise Exception('conn not initialized')
        # if isinstance(conn, dict):
        #     raise Exception('conn not initialized')

        table_name = statements.get_table_name(table)

        # https://dba.stackexchange.com/a/22368
        sql = """
        SELECT
            column_name::TEXT as name,
            column_default as default,
            UPPER(data_type) as type,
            is_nullable::boolean as nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """.format(
            table_name=table_name
        )
        raw_columns = await executors.async_raw_select(
            sql=sql, conn=conn, output_format='dict'
        )

        primary_keys = await cls._async_get_table_primary_keys(
            table_name, conn=conn
        )
        (
            unique_single_columns,
            unique_multi_columns,
        ) = await cls._async_get_unique_columns(table_name, conn=conn)
        (
            indexed_single_columns,
            indexed_multi_columns,
        ) = await cls._async_get_indexed_columns(table_name, conn=conn)
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
        cls, table_name: str, conn: spec.Connection | str | spec.DBConfig
    ) -> set[str]:
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        primary_keys_sql = """
        SELECT
            a.attname::TEXT,
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
        raw_pks = executors.raw_select(
            sql=primary_keys_sql, conn=conn, output_format='dict'
        )
        return {raw_pk['attname'] for raw_pk in raw_pks}

    @classmethod
    async def _async_get_table_primary_keys(
        cls, table_name: str, conn: spec.AsyncConnection | str | spec.DBConfig
    ) -> set[str]:
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        primary_keys_sql = """
        SELECT
            a.attname::TEXT,
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
        raw_pks = await executors.async_raw_select(
            sql=primary_keys_sql, conn=conn, output_format='dict'
        )
        return {raw_pk['attname'] for raw_pk in raw_pks}

    @classmethod
    def _get_unique_columns(
        cls, table_name: str, conn: spec.Connection | str | spec.DBConfig
    ) -> tuple[set[str], list[list[str]]]:
        # https://stackoverflow.com/a/27752061
        sql = """
        SELECT cu.constraint_name::TEXT, column_name::TEXT
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu
                on cu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE
            tc.CONSTRAINT_TYPE = 'UNIQUE'
            and tc.TABLE_NAME = '{table_name}'
        """.format(
            table_name=table_name
        )
        raw_unique = executors.raw_select(
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
    async def _async_get_unique_columns(
        cls, table_name: str, conn: spec.AsyncConnection | str | spec.DBConfig
    ) -> tuple[set[str], list[list[str]]]:
        # https://stackoverflow.com/a/27752061
        sql = """
        SELECT cu.constraint_name::TEXT, column_name::TEXT
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu
                on cu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE
            tc.CONSTRAINT_TYPE = 'UNIQUE'
            and tc.TABLE_NAME = '{table_name}'
        """.format(
            table_name=table_name
        )
        raw_unique = await executors.async_raw_select(
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
        cls, table_name: str, conn: spec.Connection | str | spec.DBConfig
    ) -> tuple[set[str], list[list[str]]]:

        sql = """
        SELECT
            t.relname::TEXT as table_name,
            i.relname::TEXT as index_name,
            a.attname::TEXT as column_name
        FROM
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        WHERE
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            and t.relname = '{table_name}'
        ORDER BY
            t.relname,
            i.relname
        """.format(
            table_name=table_name
        )
        indices = executors.raw_select(
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
    async def _async_get_indexed_columns(
        cls, table_name: str, conn: spec.AsyncConnection | str | spec.DBConfig
    ) -> tuple[set[str], list[list[str]]]:

        sql = """
        SELECT
            t.relname::TEXT as table_name,
            i.relname::TEXT as index_name,
            a.attname::TEXT as column_name
        FROM
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        WHERE
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            and t.relname = '{table_name}'
        ORDER BY
            t.relname,
            i.relname
        """.format(
            table_name=table_name
        )
        indices = await executors.async_raw_select(
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

