from __future__ import annotations

from toolsql import spec

from .. import statement_utils


def build_drop_table_statement(
    table: str | spec.TableSchema,
    *,
    if_exists: bool = True,
    dialect: spec.Dialect | None = None,
    single_line: bool = True,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_droptable.html
    - postgresql: https://www.postgresql.org/docs/current/sql-droptable.html
    """

    table_name = statement_utils.get_table_name(table)

    return """DROP TABLE {if_exists}{table_name}""".format(
        table_name=table_name,
        if_exists='IF EXISTS ' if if_exists else '',
    )

