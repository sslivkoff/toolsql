from __future__ import annotations

import typing

from toolsql import spec
from .. import statement_utils


def build_select_statement(
    *,
    dialect: spec.Dialect,
    single_line: bool = True,
    #
    # predicates
    table: str | spec.TableSchema,
    columns: spec.ColumnsExpression | None = None,
    distinct: bool = False,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, typing.Any] | None = None,
    where_ilike: typing.Mapping[str, typing.Any] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
    order_by: spec.OrderBy | None = None,
    limit: int | str | None = None,
    offset: int | str | None = None,
) -> tuple[str, spec.ExecuteParams]:
    """
    - sqlite https://www.sqlite.org/lang_select.html
    - postgresql https://www.postgresql.org/docs/current/sql-select.html
    """

    columns_str = statement_utils.build_columns_expression(
        columns=columns,
        distinct=distinct,
        dialect=dialect,
    )

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
    SELECT
        {columns}
    FROM
        {table_name}
    {where_clause}
    {order_by}
    {limit}
    {offset}
    """.format(
        columns=columns_str,
        table_name=table_name,
        where_clause=where_clause,
        order_by=_order_by_to_str(order_by),
        limit=_limit_to_str(limit),
        offset=_offset_to_str(offset),
    )

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql, parameters


def _order_by_to_str(order_by: spec.OrderBy | None) -> str:
    if order_by is None:
        return ''
    elif isinstance(order_by, (list, tuple)):
        if len(order_by) == 0:
            return ''
        else:
            items = [_order_by_item(item) for item in order_by]
            return 'ORDER BY ' + ','.join(items)
    elif isinstance(order_by, (str, dict)):
        return 'ORDER BY ' + _order_by_item(order_by)
    else:
        raise Exception('unknown order_by format: ' + str(order_by))


def _order_by_item(order_by: spec.OrderByItem) -> str:

    if order_by is None:
        return ''

    elif isinstance(order_by, str):
        if not statement_utils.is_column_name(order_by):
            raise NotImplementedError('escape user input')
        return order_by

    elif isinstance(order_by, dict):

        # determine direction
        asc = order_by.get('asc')
        desc = order_by.get('desc')
        if asc is not None and desc is not None:
            if asc == desc:
                raise Exception('conflicting asc and desc specifications')
        elif desc is not None:
            asc = not desc
        else:
            asc = True

        # create answer
        column = order_by['column']
        if not statement_utils.is_column_name(column):
            raise NotImplementedError('escape user input')
        if asc:
            return column + ' ASC'
        else:
            return column + ' DESC'

    else:
        raise Exception('')


def _limit_to_str(limit: int | str | None) -> str:
    if limit is None:
        return ''
    elif isinstance(limit, str):
        raise NotImplementedError('need to escape user input')
        return 'LIMIT ' + limit
    elif isinstance(limit, int):
        return 'LIMIT ' + str(limit)
    else:
        raise Exception('unknown limit format')


def _offset_to_str(offset: int | str | None) -> str:
    if offset is None:
        return ''
    elif isinstance(offset, int):
        return 'OFFSET ' + str(offset)
    elif isinstance(offset, str):
        raise NotImplementedError('need to escape user input')
        return 'OFFSET ' + offset
    else:
        raise Exception('unknown offset format')

