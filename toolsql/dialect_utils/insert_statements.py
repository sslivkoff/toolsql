from __future__ import annotations

import typing
from typing_extensions import Literal

from toolsql import spec


def _prepare_rows_for_insert(
    *,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    dialect: spec.Dialect,
) -> spec.ExecuteManyParams | None:

    # convert to rows
    if row is not None and rows is not None:
        raise Exception()
    elif row is not None:
        rows = [row]
    elif rows is not None:
        pass
    else:
        rows = None

    # encode json columns
    if rows is not None:
        rows = _wrap_json_columns(rows=rows, dialect=dialect)

    return rows


def _wrap_json_columns(
    *,
    rows: spec.ExecuteManyParams | None = None,
    dialect: Literal['sqlite', 'postgresql'],
) -> spec.ExecuteManyParams | None:
    if rows is not None:
        new_rows: list[typing.Any] | None = None
        for r, row in enumerate(rows):
            new_row = None
            if isinstance(row, (list, tuple)):
                for c, cell in enumerate(row):
                    if isinstance(cell, (dict, list, tuple)):
                        if new_row is None:
                            new_row = list(row)
                        new_row[c] = _wrap_json_cell(cell, dialect)
            elif isinstance(row, dict):
                for key, value in row.items():
                    if isinstance(value, (dict, list, tuple)):
                        if new_row is None:
                            new_row = row.copy()
                        new_row[key] = _wrap_json_cell(value, dialect)
            else:
                raise Exception('unknown row')

            if new_row is not None:
                if new_rows is None:
                    new_rows = list(rows)
                new_rows[r] = new_row

        if new_rows is not None:
            rows = new_rows

    return rows


def _wrap_json_cell(item: typing.Any, dialect: spec.Dialect) -> typing.Any:
    if dialect == 'postgresql':
        from psycopg.types.json import Jsonb

        return Jsonb(item)
    elif dialect == 'sqlite':
        import json

        return json.dumps(item)
    else:
        raise Exception('unknown dialect: ' + str(dialect))


def build_insert_statement(
    *,
    sql: str | None = None,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | None = None,
    columns: typing.Sequence[str] | None = None,
    dialect: Literal['sqlite', 'postgresql'],
    single_line: bool = True,
) -> tuple[str, spec.ExecuteManyParams | None]:
    """
    https://www.sqlite.org/lang_insert.html

    call styles
    - insert(sql)  # sql contains row data
    - insert(sql, row)  # sql contains placeholders for row
    - insert(sql, rows)  # sql contains placeholders for rows
    - insert(table, rows)  # build sql statement using inputs
    - insert(table, columns, rows)  # build sql statement using inputs
    """

    rows = _prepare_rows_for_insert(row=row, rows=rows, dialect=dialect)

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
        INSERT INTO {table}
        {columns_expression}
        VALUES ({values_expression})
        """.format(
            table=table,
            columns_expression=columns_expression,
            values_expression=values_expression,
        )
        sql = sql.strip()

        if single_line:
            import re

            # https://stackoverflow.com/a/1546245
            sql = re.sub('\s\s+', ' ', sql).lstrip()

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

