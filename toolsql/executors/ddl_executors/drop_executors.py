from __future__ import annotations

import typing

from toolsql import spec
from toolsql import statements


def drop_table(
    table: str,
    *,
    conn: spec.Connection,
    confirm: bool = False,
    if_exists: bool = False,
) -> None:
    """drop table from database"""

    if not confirm:
        raise Exception('to drop table use confirm=True')

    sql = statements.build_drop_table_statement(table, if_exists=if_exists)
    conn.execute(sql)


def drop_tables(
    *,
    tables: typing.Sequence[str],
    if_exists: bool = False,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:
    """drop tables from database"""

    for table in tables:
        drop_table(
            table=table,
            conn=conn,
            confirm=confirm,
            if_exists=if_exists,
        )


def drop_schema_tables(
    db_schema: spec.DBSchema,
    *,
    conn: spec.Connection,
    confirm: bool = False,
    if_exists: bool = False,
) -> None:
    """drop tables found in schema"""

    drop_tables(
        tables=list(db_schema['tables'].keys()),
        conn=conn,
        if_exists=if_exists,
        confirm=confirm,
    )

