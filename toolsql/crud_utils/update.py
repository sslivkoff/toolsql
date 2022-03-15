from .. import sqlalchemy_utils


def update(
    table,
    conn,
    update_values,
    row_id=None,
    row_ids=None,
    where_equals=None,
    where_in=None,
):

    # create statement
    statement = create_update_statement(
        table=table,
        conn=conn,
        update_values=update_values,
        row_id=row_id,
        row_ids=row_ids,
        where_equals=where_equals,
        where_in=where_in,
    )

    # execute statement
    result = conn.execute(statement)

    return result


def create_update_statement(
    table,
    update_values,
    conn=None,
    statement=None,
    row_id=None,
    row_ids=None,
    where_equals=None,
    where_in=None,
):

    # get table object
    if isinstance(table, str):
        table = sqlalchemy_utils.create_table_object_from_db(
            table_name=table, conn=conn
        )

    # create statement
    if statement is None:
        statement = table.update()

    # add where clause
    statement = sqlalchemy_utils.add_where_clause(
        statement=statement,
        table=table,
        row_id=row_id,
        row_ids=row_ids,
        where_equals=where_equals,
        where_in=where_in,
    )

    # specify update values
    statement = statement.values(**update_values)

    return statement

