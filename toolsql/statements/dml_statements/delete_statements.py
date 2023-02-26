from __future__ import annotations

import typing

from toolsql import spec
from .. import statement_utils


def build_delete_statement(
    *,
    dialect: spec.Dialect,
    single_line: bool = True,
    #
    # predicates
    table: str | spec.TableSchema,
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
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
) -> tuple[str, spec.ExecuteParams]:
    """
    - sqlite: https://www.sqlite.org/lang_delete.html
    - postgresql: https://www.postgresql.org/docs/current/sql-delete.html
    """

    table_name = statement_utils.get_table_name(table)

    where_clause, parameters = statement_utils._where_clause_to_str(
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        where_or=where_or,
        dialect=dialect,
        table=table,
    )

    sql = """
    DELETE FROM
        {table_name}
    {where_clause}
    """.format(table_name=table_name, where_clause=where_clause)

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql, parameters

