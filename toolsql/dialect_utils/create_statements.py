from __future__ import annotations

from toolsql import spec


def build_create_statement(
    table_schema: spec.TableSchema,
    *,
    if_exists: bool = True,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_createtable.html
    - postgresql: https://www.postgresql.org/docs/current/sql-createtable.html
    """

    columns = ""

    sql = """
    CREATE TABLE {table_name}
    ( {columns} )
    """
    return sql.format(table_name=table_schema['name'], columns=columns)

