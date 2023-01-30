from __future__ import annotations

from toolsql import conn_utils
from toolsql import spec
from toolsql import dialect_utils


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

    dialect = conn_utils.get_conn_dialect(conn)

    if table_only:
        sql = dialect_utils.build_create_table_statement(
            table=table,
            dialect=dialect,
            if_not_exists=if_not_exists,
        )
        conn.execute(sql)
    else:
        statements = dialect_utils.build_all_table_schema_create_statements(
            table,
            dialect=dialect,
            if_not_exists=if_not_exists,
        )
        for statement in statements:
            conn.execute(statement)

