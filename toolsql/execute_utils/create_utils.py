from __future__ import annotations

from toolsql import conn_utils
from toolsql import spec
from toolsql import dialect_utils


def create_table(
    table: spec.TableSchema,
    *,
    conn: spec.Connection,
) -> None:

    if isinstance(conn, str):
        raise Exception('conn not initialized')

    dialect = conn_utils.get_conn_dialect(conn)
    sql = dialect_utils.build_create_statement(table, dialect=dialect)
    conn.execute(sql)

