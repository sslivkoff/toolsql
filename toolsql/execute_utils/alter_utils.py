from __future__ import annotations

from toolsql import conn_utils
from toolsql import dialect_utils
from toolsql import driver_utils
from toolsql import spec


def alter_table_rename(
    *,
    old_table_name: str,
    new_table_name: str,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    sql = dialect_utils.build_alter_table_rename_statement(
        old_table_name=old_table_name,
        new_table_name=new_table_name,
    )

    # execute query
    driver = driver_utils.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)


def alter_table_rename_column(
    table_name: str,
    old_column_name: str,
    new_column_name: str,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    sql = dialect_utils.build_alter_table_rename_column_statement(
        table_name=table_name,
        old_column_name=old_column_name,
        new_column_name=new_column_name,
    )

    # execute query
    driver = driver_utils.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)


def alter_table_add_column(
    *,
    table_name: str,
    column: spec.ColumnSchema,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    dialect = conn_utils.get_conn_dialect(conn)
    sql = dialect_utils.build_alter_table_add_column_statement(
        table_name=table_name,
        column=column,
        dialect=dialect,
    )

    # execute query
    driver = driver_utils.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)


def alter_table_drop_column(
    *,
    table_name: str,
    column_name: str,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    sql = dialect_utils.build_alter_table_drop_column_statement(
        table_name=table_name,
        column_name=column_name,
    )

    # execute query
    driver = driver_utils.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)

