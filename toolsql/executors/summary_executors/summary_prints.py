from __future__ import annotations

import typing

import toolsql
from toolsql import schemas
from toolsql import spec
from .. import ddl_executors
from . import summary_usage

if typing.TYPE_CHECKING:
    import toolcli


def print_db_usage(
    target: spec.Connection | spec.DBConfig,
    *,
    styles: toolcli.StyleTheme | None = None,
    n_bytes: bool = False,
) -> None:

    import toolstr

    # get conn
    if isinstance(target, dict):
        with toolsql.connect(target) as conn:
            return print_db_usage(
                target=conn,
                styles=styles,
                n_bytes=n_bytes,
            )
    elif spec.is_sync_connection(target):
        conn = target
    else:
        raise Exception('unknown db target')

    # get table information
    tables = ddl_executors.get_table_names(conn)
    rows = []
    for table in tables:
        row_count = summary_usage.get_table_row_count(table, conn=conn)
        row = [table, row_count]
        if n_bytes:
            bytecount = summary_usage.get_table_nbytes(table=table, conn=conn)
            row.append(toolstr.format(bytecount, 'nbytes'))
        rows.append(row)

    # print table information
    labels = ['table', 'row count']
    if n_bytes:
        labels.append('n_bytes')
    toolstr.print_table(rows, labels=labels)


def print_table_schema(
    table: spec.TableSchema,
    *,
    indent: int | str | None = None,
    shorthand: bool = True,
    styles: toolcli.StyleTheme | None = None,
) -> None:

    import toolstr

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
            value = column[prop]  # type: ignore
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
        column_justify=column_justify,  # type: ignore
    )


def print_db_schema(
    target: spec.DBSchema | spec.Connection | spec.DBConfig,
    *,
    styles: toolcli.StyleTheme | None = None,
    title: str | None = None,
) -> None:

    import toolstr

    try:
        db_schema = schemas.normalize_shorthand_db_schema(target)  # type: ignore
        if title is None:
            title = 'Database Schema (Not Live Database)'
    except Exception:
        db_schema = ddl_executors.get_db_schema(target)  # type: ignore
        db_schema = schemas.normalize_shorthand_db_schema(db_schema)
        if title is None:
            title = 'Database Schema (Live Database)'

    toolstr.print_text_box(title)
    for t, table_schema in enumerate(db_schema['tables'].values()):
        if t > 0:
            print()
        print()
        print_table_schema(
            table_schema, indent=4, shorthand=False, styles=styles
        )

