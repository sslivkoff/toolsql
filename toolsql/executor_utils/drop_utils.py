from __future__ import annotations

from toolsql import spec


def drop_table(
    table_name: str,
    *,
    conn: spec.Connection,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('to drop table use confirm=True')

