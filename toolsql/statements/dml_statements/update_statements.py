from __future__ import annotations

import typing

from toolsql import spec
from .. import statement_utils


def build_update_statement(
    *,
    dialect: spec.Dialect,
    single_line: bool = True,
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
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
) -> tuple[str, spec.ExecuteParams]:
    """
    - sqlite: https://www.sqlite.org/lang_update.html
    - postgres: https://www.postgresql.org/docs/current/sql-update.html
    """

    table_name = statement_utils.get_table_name(table)

    # value set clause
    columns, value_parameters = _get_columns_and_parameters(
        columns=columns, values=values
    )
    placeholder = statement_utils.get_dialect_placeholder(dialect)
    subclauses = [column + ' = ' + placeholder for column in columns]
    value_set = ', '.join(subclauses)

    # where clause
    where_clause, where_parameters = statement_utils._where_clause_to_str(
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

    parameters = tuple(value_parameters) + where_parameters

    sql = """
    UPDATE
        {table_name}
    SET
        {value_set}
    {where_clause}
    """.format(
        table_name=table_name,
        value_set=value_set,
        where_clause=where_clause,
    )

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql, tuple(parameters)


def _get_columns_and_parameters(
    columns: typing.Sequence[str] | None,
    values: spec.ExecuteParams,
) -> tuple[typing.Sequence[str], typing.Sequence[str]]:
    if columns is None:
        if not isinstance(values, dict):
            raise Exception('must specify columns or use a dict for values')
        elif isinstance(values, (list, tuple)):
            return tuple(values.keys()), tuple(values.values())
        else:
            raise Exception('invalid format for values: ' + str(type(values)))
    elif columns is not None:
        if isinstance(values, dict):
            if not all(column in values for column in columns):
                raise Exception('not all columns in values')
            return columns, [values[column] for column in columns]
        elif isinstance(values, (list, tuple)):
            if len(values) != len(columns):
                raise Exception('mismatched number of columns vs values')
            return columns, values
        else:
            raise Exception('invalid format for values: ' + str(type(values)))

