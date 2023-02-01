from __future__ import annotations

from toolsql import drivers
from toolsql import spec
from toolsql import statements


def alter_table_rename(
    *,
    old_table: str,
    new_table: str,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    sql = statements.build_alter_table_rename_statement(
        old_table=old_table,
        new_table=new_table,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)


def alter_table_rename_column(
    table: str | spec.TableSchema,
    old_column_name: str,
    new_column_name: str,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    sql = statements.build_alter_table_rename_column_statement(
        table=table,
        old_column_name=old_column_name,
        new_column_name=new_column_name,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)


def alter_table_add_column(
    *,
    table: str | spec.TableSchema,
    column: spec.ColumnSchema,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    dialect = drivers.get_conn_dialect(conn)
    sql = statements.build_alter_table_add_column_statement(
        table=table,
        column=column,
        dialect=dialect,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)


def alter_table_drop_column(
    *,
    table: str | spec.TableSchema,
    column_name: str,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    sql = statements.build_alter_table_drop_column_statement(
        table=table,
        column_name=column_name,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.execute(conn=conn, sql=sql)

