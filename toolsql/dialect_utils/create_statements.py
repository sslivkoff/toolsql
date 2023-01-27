from __future__ import annotations

from toolsql import schema_utils
from toolsql import spec


def build_create_statement(
    table: spec.TableSchema,
    *,
    if_not_exists: bool = True,
    single_line: bool = False,
    dialect: spec.Dialect,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_createtable.html
    - postgresql: https://www.postgresql.org/docs/current/sql-createtable.html
    """

    table = schema_utils.normalize_shorthand_table_schema(table)

    columns = [
        _format_column(column, dialect=dialect) for column in table['columns']
    ]

    template = """
    CREATE TABLE {if_not_exists}
    {table_name}
    ( {columns} )
    """
    sql = template.format(
        table_name=table['name'],
        if_not_exists='IF NOT EXISTS' if if_not_exists else '',
        columns=', '.join(columns),
    )

    if single_line:
        import re

        # https://stackoverflow.com/a/1546245
        sql = re.sub('\s\s+', ' ', sql).lstrip()

    return sql.rstrip()


def _format_column(column: spec.ColumnSchema, dialect: spec.Dialect) -> str:
    columntype = schema_utils.convert_columntype_to_dialect(
        column['type'],
        dialect,
    )
    line = column['name'] + ' ' + columntype.upper()
    if column.get('primary'):
        line = line + ' PRIMARY KEY'
    return line

