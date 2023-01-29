from __future__ import annotations

import typing

from toolsql import spec
from . import statement_utils


def build_delete_statement(
    *,
    sql: str | None,
    parameters: spec.SqlParameters | None,
    dialect: spec.Dialect,
    single_line: bool = True,
    #
    # predicates
    table_name: str | None = None,
    #
    # where filters
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
) -> tuple[str, spec.SqlParameters]:
    """
    - sqlite: https://www.sqlite.org/lang_delete.html
    - postgresql: https://www.postgresql.org/docs/current/sql-delete.html
    """

    if sql is not None:
        if parameters is None:
            parameters = tuple()
        return sql, parameters

    if sql is None:
        if table_name is None:
            raise Exception('must specify table_name or raw sql')
        if not statement_utils.is_table_name(table_name):
            raise Exception('not a valid table name')

        where_clause, parameters = statement_utils._where_clause_to_str(
            where_equals=where_equals,
            where_gt=where_gt,
            where_gte=where_gte,
            where_lt=where_lt,
            where_lte=where_lte,
            where_like=where_like,
            where_ilike=where_ilike,
            where_in=where_in,
            dialect=dialect,
        )

        sql = """
        DELETE FROM
            {table_name}
        {where_clause}
        """.format(table_name=table_name, where_clause=where_clause)

        return sql, parameters

