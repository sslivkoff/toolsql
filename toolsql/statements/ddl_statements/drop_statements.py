from __future__ import annotations

from toolsql import spec


def build_drop_statement(
    table: str | spec.TableSchema,
    *,
    if_exists: bool = True,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_droptable.html
    - postgresql: https://www.postgresql.org/docs/current/sql-droptable.html
    """

    if isinstance(table, str):
        table_name = table
    elif isinstance(table, dict):
        table_name = table['name']
    else:
        raise Exception('unknown table specification: ' + str(type(table)))

    return """DROP TABLE {if_exists}{table_name}""".format(
        table_name=table_name,
        if_exists='IF EXISTS ' if if_exists else '',
    )

