from __future__ import annotations

from toolsql import spec
from toolsql import dialect_utils


def create_table(
    *,
    table_schema: spec.TableSchema,
    conn: spec.Connection,
) -> None:

    if isinstance(conn, str):
        raise Exception('conn not initialized')

    sql = dialect_utils.build_create_statement(table_schema)
    conn.execute(sql)

