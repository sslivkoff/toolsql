from __future__ import annotations

from toolsql import drivers
from toolsql import spec
from toolsql import statements


def create_table(
    table: spec.TableSchema,
    *,
    if_not_exists: bool = False,
    conn: spec.Connection,
    table_only: bool = False,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    dialect = drivers.get_conn_dialect(conn)

    if table_only:
        sql = statements.build_create_table_statement(
            table=table,
            dialect=dialect,
            if_not_exists=if_not_exists,
        )
        conn.execute(sql)
    else:
        sqls = statements.build_all_table_schema_create_statements(
            table,
            dialect=dialect,
            if_not_exists=if_not_exists,
        )
        for sql in sqls:
            conn.execute(sql)

