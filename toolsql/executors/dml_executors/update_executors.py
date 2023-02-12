from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from toolsql import statements


def update(
    *,
    conn: spec.Connection,
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    values: spec.ExecuteParams,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
) -> None:

    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_update_statement(
        dialect=dialect,
        table=table,
        columns=columns,
        values=values,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        where_or=where_or,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql, parameters=parameters)


async def async_update(
    *,
    conn: spec.AsyncConnection,
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    values: spec.ExecuteParams,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
) -> None:

    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_update_statement(
        dialect=dialect,
        table=table,
        columns=columns,
        values=values,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        where_or=where_or,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    await driver.async_execute(conn=conn, sql=sql, parameters=parameters)

