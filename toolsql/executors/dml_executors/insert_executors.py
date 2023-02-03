from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from toolsql import statements


def insert(
    *,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    conn: spec.Connection,
    on_conflict: spec.OnConflictOption | None = None,
) -> None:

    # build insert statement
    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_insert_statement(
        row=row,
        rows=rows,
        table=table,
        columns=columns,
        dialect=dialect,
        on_conflict=on_conflict,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.executemany(conn=conn, sql=sql, parameters=parameters)


async def async_insert(
    *,
    sql: str | None = None,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    conn: spec.AsyncConnection,
    on_conflict: spec.OnConflictOption | None = None,
) -> None:

    # build insert statement
    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_insert_statement(
        row=row,
        rows=rows,
        table=table,
        columns=columns,
        dialect=dialect,
        on_conflict=on_conflict,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    await driver.async_executemany(conn=conn, sql=sql, parameters=parameters)

