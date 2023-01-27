from __future__ import annotations

import typing
from typing_extensions import Literal

from toolsql import spec


def build_insert_statement(
    *,
    sql: str | None = None,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table_name: str | None = None,
    columns: typing.Sequence[str] | None = None,
    dialect: Literal['sqlite', 'postgresql'],
) -> tuple[str, spec.ExecuteManyParams | None]:
    """
    https://www.sqlite.org/lang_insert.html

    call styles
    - insert(sql)  # sql contains row data
    - insert(sql, row)  # sql contains placeholders for row
    - insert(sql, rows)  # sql contains placeholders for rows
    - insert(table_name, rows)  # build sql statement using inputs
    - insert(table_name, columns, rows)  # build sql statement using inputs
    """

    if row is not None and rows is not None:
        raise Exception()
    elif row is not None:
        rows = [row]
    elif rows is not None:
        pass
    else:
        rows = None

    if sql is not None:
        return sql, rows
    else:

        if columns is not None:
            columns_expression = '(' + ','.join(columns) + ')'
        else:
            columns_expression = ''

        values_expression = _create_values_expression(
            rows=rows,
            columns=columns,
            dialect=dialect,
        )

        sql = """
        INSERT INTO {table_name}
        {columns_expression}
        VALUES {values_expression}
        """.format(
            table_name=table_name,
            columns_expression=columns_expression,
            values_expression=values_expression,
        )

        return sql, rows


def _create_values_expression(
    *,
    rows: spec.ExecuteManyParams | None = None,
    columns: typing.Sequence[str] | None,
    dialect: Literal['sqlite', 'postgresql'],
) -> str:

    if rows is None or len(rows) == 0:
        return ''
    else:
        if isinstance(rows[0], dict):
            if dialect == 'sqlite':
                return ', '.join(':' + column for column in columns)
            elif dialect == 'postgres':
                return ', '.join('%(' + column + ')s' for column in columns)
            else:
                raise Exception('')
        elif isinstance(rows[0], (list, tuple)):
            if dialect == 'sqlite':
                return ', '.join(['?'] * len(rows[0]))
            elif dialect == 'postgresql':
                return ', '.join(['%s'] * len(rows[0]))
            else:
                raise Exception('')

