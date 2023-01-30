from __future__ import annotations

import typing
from toolsql import drivers
from toolsql import spec
from toolsql import statements


def delete(
    *,
    conn: spec.Connection,
    table_name: str | None = None,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
) -> None:

    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_delete_statement(
        dialect=dialect,
        table_name=table_name,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql, parameters=parameters)


async def async_delete(
    *,
    conn: spec.AsyncConnection,
    table_name: str | None = None,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
) -> None:

    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_delete_statement(
        dialect=dialect,
        table_name=table_name,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    await driver.async_execute(conn=conn, sql=sql, parameters=parameters)

