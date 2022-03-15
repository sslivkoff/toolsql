import sqlalchemy

from . import table_utils


def create_insert_statement(table):
    engine = table.metadata.bind
    is_postgresql = isinstance(
        engine.dialect,
        sqlalchemy.dialects.postgresql.psycopg2.PGDialect_psycopg2,
    )
    if is_postgresql:
        from sqlalchemy.dialects import postgresql

        return postgresql.insert(table=table)
    else:
        return table.insert()


def add_where_clause(
    *,
    table,
    statement,
    row_id=None,
    row_ids=None,
    where_equals=None,
    where_in=None,
    where_foreign_row_equals=None,
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


def add_order_by_clause(statement, order_by, table):
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
            item = table.c[item]
        elif isinstance(item, dict):
            item = table.c[item]['column']
            item_order = item.get('order')
            if item_order is not None:
                if item_order == 'descending':
                    item = item.desc()
                elif item_order == 'ascending':
                    item = item.asc()
                else:
                    raise Exception('unknown item order: ' + str(item_order))
        else:
            raise Exception('unknown orderby type: ' + str(type(item)))
        new_order_by.append(item)
    order_by = new_order_by

    # add to statement
    statement = statement.order_by(*order_by)

    return statement


def _create_foreign_row_equals(statement, table, where_foreign_row_equals):
    for column, foreign in where_foreign_row_equals.items():

        column = table.c[column]
        foreign_keys = column.foreign_keys
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
                    column == foreign_table.c[foreign_primary_key],
                    foreign_table.c[foreign_column] == foreign_value,
                )
            )

    return statement

