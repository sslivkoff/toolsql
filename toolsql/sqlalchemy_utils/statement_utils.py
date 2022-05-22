from __future__ import annotations

import typing

import sqlalchemy  # type: ignore

from .. import spec
from . import table_utils


def create_blank_insert_statement(
    table: spec.SATable,
    engine: spec.SAEngine | None = None,
    conn: spec.SAConnection | None = None,
    dialect: str | None = None,
) -> sqlalchemy.sql.expression.Insert:

    if dialect is None:
        # determine if dialect is available
        if engine is None:
            if conn is not None:
                engine = conn.engine
            else:
                return table.insert()

        # determine dialect
        dialect = engine.dialect.__module__.split('.')[2]

    # return statement based on dialect
    if dialect == 'postgresql':
        from sqlalchemy.dialects import postgresql  # type: ignore

        return postgresql.insert(table=table)

    elif dialect == 'sqlite':
        from sqlalchemy.dialects import sqlite  # type: ignore

        return sqlite.insert(table=table)

    elif dialect is None:
        return table.insert()

    else:
        raise Exception('unknown dialect format')


def add_where_clause(
    *,
    table: spec.SATable,
    statement: spec.SAStatement,
    row_id: typing.Any | None = None,
    row_ids: typing.Sequence[typing.Any] | None = None,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None = None,
    where_foreign_row_equals: typing.Mapping[
        str, typing.Mapping[str, typing.Any]
    ]
    | None = None,
    where_gt=None,
    where_gte=None,
    where_lt=None,
    where_lte=None,
    where_start_of=None,
    filters=None,
    filter_by=None,
):
    if row_id is not None:
        primary_key = table_utils.get_table_primary_key(table)
        statement = statement.where(table.c[primary_key] == row_id)
    if row_ids is not None:
        primary_key = table_utils.get_table_primary_key(table)
        statement = statement.where(table.c[primary_key].in_(row_ids))
    if where_equals is not None:
        for column_name, column_value in where_equals.items():
            statement = statement.where(table.c[column_name] == column_value)
    if where_in is not None:
        for column_name, value_list in where_in.items():
            statement = statement.where(table.c[column_name].in_(value_list))
    if where_foreign_row_equals is not None:
        statement = _create_foreign_row_equals(
            statement=statement,
            table=table,
            where_foreign_row_equals=where_foreign_row_equals,
        )
    if where_lt is not None:
        for column_name, column_value in where_lt.items():
            statement = statement.where(table.c[column_name] < column_value)
    if where_lte is not None:
        for column_name, column_value in where_lte.items():
            statement = statement.where(table.c[column_name] <= column_value)
    if where_gt is not None:
        for column_name, column_value in where_gt.items():
            statement = statement.where(table.c[column_name] > column_value)
    if where_gte is not None:
        for column_name, column_value in where_gte.items():
            statement = statement.where(table.c[column_name] >= column_value)
    if where_start_of is not None:
        for column_name, full_value in where_start_of.items():
            bound_name = 'started_by_' + column_name
            if bound_name in table.c:
                raise Exception('cannot create bound name')
            bound = sqlalchemy.bindparam(bound_name, full_value)
            statement = statement.where(bound.startswith(table.c[column_name]))

    # sqlalchemy-specific filters
    if filters is not None:
        for filter in filters:
            statement = statement.where(filter)
    if filter_by is not None:
        raise Exception()
        # statement = statement.filter_by(**filter_by)

    return statement


def add_order_by_clause(
    statement: spec.SAStatement,
    order_by: str | typing.Sequence,
    table: spec.SATable,
) -> spec.SAStatement:
    """
    - see https://docs.sqlalchemy.org/en/14/core/tutorial.html#ordering-grouping-limiting-offset-ing
    """

    # specify ordering
    if not isinstance(order_by, (list, tuple)):
        order_by = [order_by]

    # parse order_by items
    new_order_by = []
    for item in order_by:
        if isinstance(item, str):
            data = table.c[item]
        elif isinstance(item, dict):
            data = table.c[item['column']]
            item_order = item.get('order')
            if item_order is not None:
                if item_order == 'descending':
                    data = data.desc()
                elif item_order == 'ascending':
                    data = data.asc()
                else:
                    raise Exception('unknown item order: ' + str(item_order))
        else:
            raise Exception('unknown orderby type: ' + str(type(item)))
        new_order_by.append(data)
    order_by = new_order_by

    # add to statement
    statement = statement.order_by(*order_by)

    return statement


def _create_foreign_row_equals(
    statement: spec.SAStatement,
    table: spec.SATable,
    where_foreign_row_equals: typing.Mapping[
        str, typing.Mapping[str, typing.Any]
    ],
) -> spec.SAStatement:
    for column, foreign in where_foreign_row_equals.items():

        column_obj = table.c[column]
        foreign_keys = column_obj.foreign_keys
        if len(foreign_keys) != 1:
            raise Exception('must have singular foreign key')
        foreign_primary_column = next(iter(foreign_keys)).column
        foreign_table = foreign_primary_column.table
        foreign_primary_key = table_utils.get_table_primary_key(
            foreign_table,
        )

        for foreign_column, foreign_value in foreign.items():
            statement = statement.where(
                sqlalchemy.and_(
                    column_obj == foreign_table.c[foreign_primary_key],
                    foreign_table.c[foreign_column] == foreign_value,
                )
            )

    return statement
