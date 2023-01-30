from __future__ import annotations

import typing
from toolsql import conn_utils
from toolsql import dialect_utils
from toolsql import driver_utils
from toolsql import spec


def insert(
    *,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | None = None,
    columns: typing.Sequence[str] | None = None,
    conn: spec.Connection,
) -> None:

    # build insert statement
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_insert_statement(
        row=row,
        rows=rows,
        table=table,
        columns=columns,
        dialect=dialect,
    )

    # execute query
    driver = driver_utils.get_driver_class(conn=conn)
    driver.executemany(conn=conn, sql=sql, parameters=parameters)


async def async_insert(
    *,
    sql: str | None = None,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | None = None,
    columns: typing.Sequence[str] | None = None,
    conn: spec.AsyncConnection,
) -> None:

    # build insert statement
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_insert_statement(
        row=row,
        rows=rows,
        table=table,
        columns=columns,
        dialect=dialect,
    )

    # execute query
    driver = driver_utils.get_driver_class(conn=conn)
    await driver.async_executemany(conn=conn, sql=sql, parameters=parameters)

