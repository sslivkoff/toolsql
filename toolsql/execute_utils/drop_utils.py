from __future__ import annotations

from toolsql import spec
from toolsql import dialect_utils


def drop_table(
    table: str,
    *,
    conn: spec.Connection,
    confirm: bool = False,
    if_exists: bool = False,
) -> None:

    if not confirm:
        raise Exception('to drop table use confirm=True')

    sql = dialect_utils.build_drop_statement(table, if_exists=if_exists)
    conn.execute(sql)

