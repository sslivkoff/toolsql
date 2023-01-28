from __future__ import annotations

import typing

from toolsql import spec


class AbstractDbms:

    #
    # # sync
    #

    @classmethod
    def get_table_schemas(
        cls, conn: spec.Connection
    ) -> typing.Mapping[str, spec.TableSchema]:
        return {
            table_name: cls.get_table_schema(table_name, conn=conn)
            for table_name in cls.get_tables_names(conn=conn)
        }

    @classmethod
    def get_tables_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        raise Exception('get_tables_names() for ' + cls.__name__)

    @classmethod
    def get_indices_names(cls, conn: spec.Connection) -> typing.Sequence[str]:
        raise Exception('get_indices_names() for ' + cls.__name__)

    @classmethod
    def get_table_schema(
        cls, table_name: str, conn: spec.Connection
    ) -> spec.TableSchema:
        raise Exception('get_table_schema() for ' + cls.__name__)

    @classmethod
    def get_table_create_statement(
        cls, table_name: str, *, conn: spec.Connection
    ) -> str:
        raise Exception('get_table_create_statement() for ' + cls.__name__)

    #
    # # async versions
    #

    @classmethod
    async def async_get_table_schemas(
        cls, conn: spec.Connection
    ) -> typing.Mapping[str, spec.TableSchema]:
        return cls.get_table_schemas(conn=conn)

    @classmethod
    async def async_get_tables_names(
        cls, conn: spec.Connection
    ) -> typing.Sequence[str]:
        return cls.get_tables_names(conn=conn)

    @classmethod
    async def async_get_indices_names(
        cls, conn: spec.Connection
    ) -> typing.Sequence[str]:
        return cls.get_indices_names(conn=conn)

    @classmethod
    async def async_get_table_schema(
        cls, table_name: str, conn: spec.Connection
    ) -> spec.TableSchema:
        return cls.get_table_schema(
            table_name=table_name, conn=conn
        )

    @classmethod
    async def async_get_table_create_statement(
        cls, table_name: str, *, conn: spec.Connection
    ) -> str:
        return cls.get_table_create_statement(
            table_name=table_name, conn=conn
        )

