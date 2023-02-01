"""
- sqlite: https://www.sqlite.org/lang_altertable.html
- postgresql:
"""

from __future__ import annotations

from toolsql import spec

from .. import statement_utils
from . import create_statements


def build_alter_table_rename_statement(
    *,
    old_table: str | spec.TableSchema,
    new_table: str | spec.TableSchema,
    dialect: spec.Dialect | None = None,
) -> str:

    old_table_name = statement_utils.get_table_name(old_table)
    new_table_name = statement_utils.get_table_name(new_table)

    return 'ALTER TABLE {old_table_name} RENAME TO {new_table_name}'.format(
        old_table_name=old_table_name,
        new_table_name=new_table_name,
    )


def build_alter_table_rename_column_statement(
    *,
    table: str | spec.TableSchema,
    old_column_name: str,
    new_column_name: str,
    dialect: spec.Dialect | None = None,
) -> str:

    table_name = statement_utils.get_table_name(table)

    if not statement_utils.is_table_name(table_name):
        raise Exception('not a valid table name: ' + str(table_name))
    if not statement_utils.is_column_name(old_column_name):
        raise Exception('not a valid column name: ' + str(old_column_name))
    if not statement_utils.is_column_name(new_column_name):
        raise Exception('not a valid column name: ' + str(new_column_name))

    return """
    ALTER TABLE {table_name}
    RENAME COLUMN {old_column_name} TO {new_column_name}
    """.format(
        table_name=table_name,
        old_column_name=old_column_name,
        new_column_name=new_column_name,
    )


def build_alter_table_add_column_statement(
    *,
    table: str | spec.TableSchema,
    column: spec.ColumnSchema,
    dialect: spec.Dialect,
) -> str:

    table_name = statement_utils.get_table_name(table)
    column_str = create_statements._format_column(
        column=column,
        dialect=dialect,
    )
    return """
    ALTER TABLE {table_name}
    ADD COLUMN {column_str}
    """.format(
        table_name=table_name, column_str=column_str
    )


def build_alter_table_drop_column_statement(
    *,
    table: str | spec.TableSchema,
    column_name: str,
    dialect: spec.Dialect | None = None,
) -> str:

    table_name = statement_utils.get_table_name(table)
    if not statement_utils.is_table_name(table_name):
        raise Exception('not a valid table name: ' + str(table_name))
    if not statement_utils.is_column_name(column_name):
        raise Exception('not a valid column name: ' + str(column_name))

    return 'ALTER TABLE {table_name} DROP COLUMN {column_name}'.format(
        table_name=table_name,
        column_name=column_name,
    )

