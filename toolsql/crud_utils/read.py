from __future__ import annotations

import typing

from toolsql import exceptions
from .. import sqlalchemy_utils


def select(
    table,
    conn,
    row_id=None,
    row_ids=None,
    where_equals=None,
    where_in=None,
    where_foreign_row_equals=None,
    where_gt=None,
    where_gte=None,
    where_lt=None,
    where_lte=None,
    where_start_of=None,
    filters=None,
    filter_by=None,
    only_columns=None,
    sql_functions=None,
    order_by=None,
    include_id=False,
    row_count=None,
    row_format=None,
    return_count=None,
    raise_if_table_dne=True,
) -> typing.Any:
    if row_format is None:
        row_format = 'dict'
    if row_format == 'only_column' and len(only_columns) > 1:
        raise Exception('row_format="only_column" needs len(only_columns) == 1')
    if return_count is None:
        return_count = 'all'

    # get table object
    if isinstance(table, str):
        try:
            table = sqlalchemy_utils.create_table_object_from_db(
                table_name=table,
                conn=conn,
            )
        except exceptions.TableNotFound as exception:
            if raise_if_table_dne:
                raise exception
            else:
                return None

    # create statement
    statement = create_select_statement(
        table=table,
        conn=conn,
        row_id=row_id,
        row_ids=row_ids,
        where_equals=where_equals,
        where_in=where_in,
        where_foreign_row_equals=where_foreign_row_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_start_of=where_start_of,
        filters=filters,
        filter_by=filter_by,
        only_columns=only_columns,
        sql_functions=sql_functions,
        order_by=order_by,
    )

    # execute statement
    result = conn.execute(statement)

    # get return data
    return _process_select_result(
        result=result,
        row_count=row_count,
        row_format=row_format,
        return_count=return_count,
        include_id=include_id,
        table=table,
    )


def create_select_statement(
    table,
    conn=None,
    statement=None,
    row_id=None,
    row_ids=None,
    where_equals=None,
    where_in=None,
    where_foreign_row_equals=None,
    where_gt=None,
    where_gte=None,
    where_lt=None,
    where_lte=None,
    where_start_of=None,
    filters=None,
    filter_by=None,
    order_by=None,
    only_columns=None,
    sql_functions=None,
):

    # get table object
    if isinstance(table, str):
        table = sqlalchemy_utils.create_table_object_from_db(
            table_name=table, conn=conn
        )

    # create statement
    if statement is None:
        statement = table.select()

    # select by id
    statement = sqlalchemy_utils.add_where_clause(
        table=table,
        statement=statement,
        row_id=row_id,
        row_ids=row_ids,
        where_equals=where_equals,
        where_in=where_in,
        where_foreign_row_equals=where_foreign_row_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_start_of=where_start_of,
        filters=filters,
        filter_by=filter_by,
    )

    # order results
    if order_by is not None:
        statement = sqlalchemy_utils.add_order_by_clause(
            statement=statement,
            table=table,
            order_by=order_by,
        )

    # select columns to return
    if sql_functions is not None:
        import sqlalchemy  # type: ignore

        if only_columns is not None:
            raise NotImplementedError(
                'cannot use sql_functions and only_columns'
            )
        functions_as_columns = []
        for function_name, column_name in sql_functions:
            function = getattr(sqlalchemy.func, function_name)
            column = table.columns[column_name]
            expression = function(column)
            expression = expression.label(function_name + '__' + column_name)
            functions_as_columns.append(expression)
        statement = statement.with_only_columns(*functions_as_columns)
    if only_columns is not None:
        if isinstance(only_columns[0], str):
            only_columns = [table.columns[column] for column in only_columns]
        statement = statement.with_only_columns(*only_columns)

    return statement


def _process_select_result(
    result,
    row_count,
    row_format,
    return_count,
    include_id,
    table,
):
    """process sql query output into desired result"""

    # check row count
    if row_count is not None:
        _check_row_count(result=result, row_count=row_count)

    # get primary key
    if include_id:
        primary_key = sqlalchemy_utils.get_table_primary_key(table)

    # get return data
    if return_count == 'one':
        row = result.fetchone()
        formatted_row = _format_row(row, row_format)
        if include_id:
            return {row[primary_key]: formatted_row}
        else:
            return formatted_row

    elif return_count == 'all':
        rows = result.fetchall()
        formatted_rows = [_format_row(row, row_format) for row in rows]
        if include_id:
            return {
                row[primary_key]: formatted_row
                for row, formatted_row in zip(rows, formatted_rows)
            }
        else:
            return [formatted_row for formatted_row in formatted_rows]
    else:
        raise Exception('unknown return_count: ' + str(return_count))


def _check_row_count(result, row_count):
    """verify that an acceptable number of rows as found"""
    if row_count == 'exactly_one':
        if result.rowcount < 1:
            raise Exception('no rows found, expected exactly one')
        if result.rowcount > 1:
            raise Exception('more than one row found, expected exactly one')
    elif row_count == 'at_least_one':
        if result.rowcount < 1:
            raise Exception('no rows found, expected at least one')
    elif row_count == 'at_most_one':
        if result.rowcount > 1:
            raise Exception('more than one row found, expected at most one')
    else:
        raise Exception('unknown row_count: ' + str(row_count))


def _format_row(row, row_format, column=None):
    """format a row into the specified python type"""
    if row is None:
        return row

    if row_format == 'dict':
        return sqlalchemy_utils.row_to_dict(row)
    elif row_format == 'list':
        raise NotImplementedError()
    elif row_format == 'object':
        return row
    elif row_format == 'only_column':
        if column is None:
            columns = row._fields
            if len(columns) != 1:
                raise Exception('number of rows must equal 1')
            else:
                column = columns[0]
        return getattr(row, column)
    else:
        raise Exception('unknown row_format: ' + str(row_format))
