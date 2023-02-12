from __future__ import annotations

import typing

from toolsql import schemas
from toolsql import spec
from .. import statement_utils


def build_create_table_statement(
    table: spec.TableSchema,
    *,
    if_not_exists: bool = False,
    single_line: bool = True,
    dialect: spec.Dialect,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_createtable.html
    - postgresql: https://www.postgresql.org/docs/current/sql-createtable.html
    """

    table = schemas.normalize_shorthand_table_schema(table)

    columns_str = _format_columns(columns=table['columns'], dialect=dialect)

    template = """
    CREATE TABLE {if_not_exists}
    {table_name}
    ( {columns} )
    """
    sql = template.format(
        table_name=table['name'],
        if_not_exists='IF NOT EXISTS' if if_not_exists else '',
        columns=columns_str,
    )

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql.rstrip()


def _format_columns(
    columns: typing.Sequence[spec.ColumnSchema],
    dialect: spec.Dialect,
) -> str:

    primary_columns = [
        column['name'] for column in columns if column.get('primary')
    ]
    composite_primary_key = len(primary_columns) > 1

    column_strs = [
        _format_column(
            column,
            dialect=dialect,
            composite_primary_key=composite_primary_key,
        )
        for column in columns
    ]

    if composite_primary_key:
        primary = 'PRIMARY KEY ({})'.format(', '.join(primary_columns))
        column_strs.append(primary)

    return ', '.join(column_strs)


def _format_column(
    column: spec.ColumnSchema,
    dialect: spec.Dialect,
    composite_primary_key: bool = False,
) -> str:

    columntype = schemas.convert_columntype_to_dialect(
        column['type'],
        dialect,
    )
    line = column['name'] + ' ' + columntype.upper()

    if column['primary'] and not composite_primary_key:
        line = line + ' PRIMARY KEY'

    if not column['nullable']:
        line = line + ' NOT NULL'

    if column['unique']:
        line = line + ' UNIQUE'

    return line


def build_create_index_statement(
    table_name: str,
    column_name: str | None = None,
    *,
    column_names: typing.Sequence[str] | None = None,
    index_name: str | None = None,
    if_not_exists: bool = False,
    single_line: bool = True,
    dialect: spec.Dialect,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_createindex.html
    - postgresql: https://www.postgresql.org/docs/current/sql-createindex.html
    """

    if column_name is not None and column_names is not None:
        raise Exception('specify only column_name or column_names')
    elif column_name is not None:
        columns: typing.Sequence[str] = [column_name]
    elif column_names is not None:
        columns = column_names
    else:
        raise Exception('specify column_name or column_names')

    if index_name is None:
        index_name = 'index___' + table_name + '___' + '__'.join(columns)

    template = """
    CREATE INDEX
    {if_not_exists}
    {index_name}
    ON
    {table_name}
    ( {columns} )
    """
    sql = template.format(
        if_not_exists=('IF NOT EXISTS' if if_not_exists else ''),
        index_name=index_name,
        table_name=table_name,
        columns=', '.join(columns),
    )

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql


def build_all_table_schema_create_statements(
    table_schema: spec.TableSchema,
    *,
    dialect: spec.Dialect,
    if_not_exists: bool = False,
    single_line: bool = True,
) -> typing.Sequence[str]:

    create_table = build_create_table_statement(
        table_schema,
        dialect=dialect,
        if_not_exists=if_not_exists,
        single_line=single_line,
    )

    create_index_statements = [
        build_create_index_statement(
            table_name=table_schema['name'],
            column_name=column['name'],
            dialect=dialect,
            if_not_exists=if_not_exists,
            single_line=single_line,
        )
        for column in table_schema['columns']
        if column['index']
    ]

    return [create_table] + create_index_statements

