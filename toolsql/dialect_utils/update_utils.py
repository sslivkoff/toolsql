from __future__ import annotations

import typing

from toolsql import spec

from . import statement_utils


def build_update_statement(
    *,
    sql: str | None,
    parameters: spec.SqlParameters | None,
    dialect: spec.Dialect,
    single_line: bool = True,
    #
    table_name: str | None = None,
    columns: typing.Sequence[str] | None = None,
    values: typing.Mapping[str, typing.Any] | typing.Sequence[typing.Any],
    #
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
    - sqlite: https://www.sqlite.org/lang_update.html
    - postgres: https://www.postgresql.org/docs/current/sql-update.html
    """

    if sql is not None:
        if parameters is None:
            parameters = tuple()
        return sql, parameters

    else:

        if table_name is None:
            raise Exception('must specify table_name or raw sql')
        if not statement_utils.is_table_name(table_name):
            raise Exception('not a valid table name')

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
            dialect=dialect,
        )

        parameters = tuple(value_parameters) + where_parameters

        sql = """
        UPDATE
            {table_name}
        SET
            {value_set}
        WHERE
            {where_clause}
        """.format(
            table_name=table_name,
            value_set=value_set,
            where_clause=where_clause,
        )

        return sql, tuple(parameters)


def _get_columns_and_parameters(
    columns: typing.Sequence[str] | None,
    values: typing.Mapping[str, typing.Any] | typing.Sequence[typing.Any],
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

