from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from toolsql import statements
from .. import dml_executors


#
# # row counts
#


def get_table_row_count(
    table: str | spec.TableSchema,
    *,
    conn: spec.Connection,
) -> int:

    result: int = dml_executors.select(
        table=table,
        conn=conn,
        columns=['COUNT(*)'],
        output_format='cell',
    )
    return result


async def async_get_table_row_count(
    table: str | spec.TableSchema,
    *,
    conn: spec.AsyncConnection,
) -> int:

    result: int = await dml_executors.async_select(
        table=table,
        conn=conn,
        columns=['COUNT(*)'],
        output_format='cell',
    )
    return result


#
# # byte counts
#


def get_table_nbytes(
    table: str | spec.TableSchema,
    *,
    conn: spec.Connection,
) -> int:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_table_nbytes_statement(table, dialect=dialect)
    result: int = dml_executors.raw_select(
        sql=sql, conn=conn, output_format='cell'
    )
    return result


async def async_get_table_nbytes(
    table: str | spec.TableSchema,
    *,
    conn: spec.AsyncConnection,
) -> int:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_table_nbytes_statement(table, dialect=dialect)
    result: int = await dml_executors.async_raw_select(
        sql=sql, conn=conn, output_format='cell'
    )
    return result


def get_tables_nbytes(*, conn: spec.Connection) -> typing.Mapping[str, int]:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_tables_nbytes_statement(dialect=dialect)
    result = dml_executors.raw_select(sql=sql, conn=conn, output_format='tuple')
    return dict(result)  # type: ignore


async def async_get_tables_nbytes(
    *, conn: spec.AsyncConnection
) -> typing.Mapping[str, int]:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_tables_nbytes_statement(dialect=dialect)
    result = await dml_executors.async_raw_select(
        sql=sql, conn=conn, output_format='tuple'
    )
    return dict(result)  # type: ignore

