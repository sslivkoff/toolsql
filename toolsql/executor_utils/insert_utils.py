from __future__ import annotations

import typing
from .. import conn_utils
from .. import dialect_utils
from .. import spec


def insert(
    *,
    sql: str | None = None,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table_name: str | None = None,
    columns: typing.Sequence[str] | None = None,
    conn: spec.Connection,
) -> None:

    # build insert statement
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_insert_statement(
        sql=sql,
        row=row,
        rows=rows,
        table_name=table_name,
        columns=columns,
        dialect=dialect,
    )

    # execute query
    driver = conn_utils.get_conn_driver(conn)
    driver.executemany(conn=conn, sql=sql, parameters=parameters)


async def async_insert(
    *,
    sql: str | None = None,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table_name: str | None = None,
    columns: typing.Sequence[str] | None = None,
    conn: spec.AsyncConnection,
) -> None:

    # build insert statement
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_insert_statement(
        sql=sql,
        row=row,
        rows=rows,
        table_name=table_name,
        columns=columns,
        dialect=dialect,
    )

    # execute query
    driver = conn_utils.get_conn_driver(conn)
    await driver.async_executemany(conn=conn, sql=sql, parameters=parameters)

