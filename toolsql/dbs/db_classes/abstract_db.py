from __future__ import annotations

import typing

import toolsql
from toolsql import spec
from toolsql import statements


class AbstractDb:
    def __init__(self) -> None:
        raise Exception(
            'do not initialize '
            + str(type(self))
            + ', use class methods instead'
        )

    #
    # # sync
    #

    @classmethod
    def create_db(cls, db_config: spec.DBConfig) -> None:
        raise NotImplementedError('create_db()')

    @classmethod
    def get_table_schemas(
        cls, target: spec.Connection | spec.DBConfig
    ) -> typing.Mapping[str, spec.TableSchema]:
        if isinstance(target, dict):
            with toolsql.connect(target) as conn:
                return cls.get_table_schemas(conn)
        elif spec.is_sync_connection(target):
            table_names = cls.get_table_names(conn=target, permission='read')
            return {
                table_name: cls.get_table_schema(table_name, conn=target)
                for table_name in table_names
            }
        else:
            raise Exception('unknown connection target')

    @classmethod
    def get_table_raw_column_types(
        cls,
        table: str | spec.TableSchema,
        conn: spec.Connection | str | spec.DBConfig,
    ) -> typing.Mapping[str, str]:
        table_schema = cls.get_table_schema(table=table, conn=conn)
        return {
            column_spec['name']: column_spec['type']
            for column_spec in table_schema['columns']
        }

    @classmethod
    def has_table(
        cls, table: str | spec.TableSchema, conn: spec.Connection
    ) -> bool:
        table_name = statements.get_table_name(table)
        return table_name in cls.get_table_names(conn=conn, permission=None)

    @classmethod
    def get_table_names(
        cls,
        conn: spec.Connection,
        *,
        permission: spec.TablePermission = 'read',
    ) -> typing.Sequence[str]:
        raise Exception('get_table_names() for ' + cls.__name__)

    @classmethod
    def get_indices_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        raise Exception('get_indices_names() for ' + cls.__name__)

    @classmethod
    def get_table_schema(
        cls,
        table: str | spec.TableSchema,
        conn: spec.Connection | str | spec.DBConfig,
    ) -> spec.TableSchema:
        raise Exception('get_table_schema() for ' + cls.__name__)

    @classmethod
    def get_table_create_statement(
        cls, table: str | spec.TableSchema, *, conn: spec.Connection
    ) -> str:
        raise Exception('get_table_create_statement() for ' + cls.__name__)

    #
    # # async versions
    #

    @classmethod
    async def async_get_table_schemas(
        cls, conn: spec.Connection
    ) -> typing.Mapping[str, spec.TableSchema]:
        return cls.get_table_schemas(conn)

    @classmethod
    async def async_get_table_raw_column_types(
        cls,
        table: str | spec.TableSchema,
        conn: spec.AsyncConnection | str | spec.DBConfig,
    ) -> typing.Mapping[str, str]:
        table_schema = await cls.async_get_table_schema(table=table, conn=conn)
        return {
            column_spec['name']: column_spec['type']
            for column_spec in table_schema['columns']
        }

    @classmethod
    async def async_has_table(
        cls, table: str | spec.TableSchema, conn: spec.AsyncConnection
    ) -> bool:
        table_name = statements.get_table_name(table)
        table_names = await cls.async_get_table_names(
            conn=conn, permission=None
        )
        return table_name in table_names

    @classmethod
    async def async_get_table_names(
        cls,
        conn: spec.AsyncConnection,
        *,
        permission: spec.TablePermission = 'read',
    ) -> typing.Sequence[str]:
        raise Exception('async_get_table_names() for ' + cls.__name__)

    @classmethod
    async def async_get_indices_names(
        cls, conn: spec.Connection
    ) -> typing.Sequence[str]:
        return cls.get_indices_names(conn=conn)

    @classmethod
    async def async_get_table_schema(
        cls,
        table: str | spec.TableSchema,
        conn: spec.AsyncConnection | str | spec.DBConfig,
    ) -> spec.TableSchema:
        raise Exception('async_get_table_schema() for ' + cls.__name__)

    #
    # # helper
    #

    @classmethod
    def _sort_single_multi_column_indices(
        cls, index_columns: typing.Mapping[str, typing.Sequence[str]]
    ) -> tuple[set[str], list[list[str]]]:
        single_column_indices: set[str] = set()
        multicolumn_indices: list[list[str]] = list()
        for columns in index_columns.values():
            if len(columns) == 1:
                single_column_indices.add(next(iter(columns)))
            else:
                multicolumn_indices.append(list(columns))
        return single_column_indices, multicolumn_indices

