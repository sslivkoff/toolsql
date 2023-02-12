from __future__ import annotations

import typing

import toolstr
from toolsql import schemas
from toolsql import spec
from .. import ddl_executors
from . import summary_usage


def print_db_usage(conn: spec.Connection) -> None:
    tables = ddl_executors.get_table_names(conn)
    rows = []
    for table in tables:
        row_count = summary_usage.get_table_row_count(table, conn=conn)
        row = [table, row_count]
        rows.append(row)
    labels = [
        'table',
        'row count',
    ]
    toolstr.print_table(rows, labels=labels)


def print_table_schema(
    table: spec.TableSchema,
    *,
    indent: int | str | None = None,
    shorthand: bool = True,
) -> None:

    if shorthand:
        table = schemas.normalize_shorthand_table_schema(table)

    props = [
        'name',
        'type',
        'primary',
        'index',
        'unique',
        'nullable',
        'default',
    ]
    rows: list[list[typing.Any]] = []
    for column in table['columns']:
        row = []
        for prop in props:
            value = column[prop]
            if isinstance(value, bool):
                if value:
                    row.append('✓')
                else:
                    row.append('')
            else:
                row.append(column[prop])  # type: ignore
        rows.append(row)
    toolstr.print_text_box('Table schema: ' + str(table['name']), indent=indent)
    column_justify = {prop: 'center' for prop in props}
    column_justify['name'] = 'right'
    column_justify['type'] = 'right'
    toolstr.print_table(
        rows=rows,
        labels=props,
        indent=indent,
        column_justify=column_justify,
    )


def print_db_schema(db_schema: spec.DBSchema) -> None:

    db_schema = schemas.normalize_shorthand_db_schema(db_schema)

    toolstr.print_text_box('Database Schema')
    for t, table_schema in enumerate(db_schema['tables']):
        if t > 0:
            print()
        print()
        print_table_schema(table_schema, indent=4, shorthand=False)

