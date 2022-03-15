from .. import sqlalchemy_utils


def delete(
    table,
    conn,
    row_id=None,
    row_ids=None,
    where_equals=None,
    where_in=None,
    force=False,
):
    if (
        not force
        and row_id is None
        and row_ids is None
        and (where_equals is None or len(where_equals) == 0)
        and (where_in is None or len(where_in) == 0)
    ):
        raise Exception('really delete all rows? use force=True')

    # create statement
    statement = create_delete_statement(
        table=table,
        conn=conn,
        row_id=row_id,
        row_ids=row_ids,
        where_equals=where_equals,
        where_in=where_in,
    )

    # execute statement
    result = conn.execute(statement)

    return {
        'n_rows_deleted': result.rowcount,
        'result_object': result,
    },


def create_delete_statement(
    table,
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

    # delete statement
    if statement is None:
        statement = table.delete()

    # add where clause
    statement = sqlalchemy_utils.add_where_clause(
        table=table,
        statement=statement,
        row_id=row_id,
        row_ids=row_ids,
        where_equals=where_equals,
        where_in=where_in,
    )

    return statement

