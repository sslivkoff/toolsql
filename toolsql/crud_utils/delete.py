from __future__ import annotations

import typing

from .. import sqlalchemy_utils


def delete(
    table,
    conn,
    row_id=None,
    row_ids=None,
    where_equals=None,
    where_in=None,
    where_gt=None,
    where_gte=None,
    where_lt=None,
    where_lte=None,
    force=False,
) -> typing.Any:
    if (
        not force
        and row_id is None
        and row_ids is None
        and (where_equals is None or len(where_equals) == 0)
        and (where_in is None or len(where_in) == 0)
        and (where_lt is None or len(where_lt) == 0)
        and (where_lte is None or len(where_lte) == 0)
        and (where_gt is None or len(where_gt) == 0)
        and (where_gte is None or len(where_gte) == 0)
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
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
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
    where_gt=None,
    where_gte=None,
    where_lt=None,
    where_lte=None,
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
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
    )

    return statement

