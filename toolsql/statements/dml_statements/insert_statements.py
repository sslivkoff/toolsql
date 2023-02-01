from __future__ import annotations

import typing
from typing_extensions import Literal

from toolsql import formats
from toolsql import spec
from .. import statement_utils


def build_insert_statement(
    *,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    dialect: Literal['sqlite', 'postgresql'],
    single_line: bool = True,
    on_conflict: spec.OnConflictOption | None = None,
) -> tuple[str, spec.ExecuteManyParams | None]:
    """
    - sqlite: https://www.sqlite.org/lang_insert.html
    - postgresql: https://www.postgresql.org/docs/current/sql-insert.html
    """

    table_name = statement_utils.get_table_name(table)

    rows = _prepare_rows_for_insert(
        row=row,
        rows=rows,
        dialect=dialect,
    )

    conflict_expression = _create_conflict_expression(
        on_conflict=on_conflict,
        dialect=dialect,
        columns=columns,
        rows=rows,
    )

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
    VALUES ({values_expression})
    {conflict_expression}
    """.format(
        table_name=table_name,
        columns_expression=columns_expression,
        values_expression=values_expression,
        conflict_expression=conflict_expression,
    )
    sql = sql.strip()

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql, rows


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
        rows = formats.encode_json_columns(rows=rows, dialect=dialect)

    return rows


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

            if columns is None:
                columns = list(rows[0].keys())

            if dialect == 'sqlite':
                return ', '.join(':' + column for column in columns)
            elif dialect == 'postgresql':
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

        else:
            raise Exception('invalid row format: ' + str(type(rows)))


def _create_conflict_expression(
    *,
    on_conflict: spec.OnConflictOption | None = None,
    dialect: Literal['sqlite', 'postgresql'],
    columns: typing.Sequence[str] | None,
    rows: spec.ExecuteManyParams | None,
) -> str:
    if on_conflict is None:
        return ''
    elif on_conflict == 'ignore':
        if dialect == 'sqlite':
            return 'ON CONFLICT IGNORE'
        elif dialect == 'postgresql':
            return 'ON CONFLICT DO NOTHING'
        else:
            raise Exception('unknown dialect: ' + str(dialect))
    elif on_conflict == 'update':
        if dialect == 'sqlite':
            return 'ON CONFLICT REPLACE'
        elif dialect == 'postgresql':
            if columns is None:
                if rows is None or len(rows) == 0:
                    return ''
                else:
                    if isinstance(rows[0], dict):
                        columns = list(rows[0].keys())
                    else:
                        # see https://stackoverflow.com/questions/40687267/how-to-update-all-columns-with-insert-on-conflict
                        raise Exception(
                            'must explicitly specify columns for postgresql upserts'
                        )
            for column in columns:
                if not statement_utils.is_column_name(column):
                    raise Exception('not a valid column name: ' + str(column))
            column_updates = [
                column + ' = EXCLUDED.' + column for column in columns
            ]
            return 'ON CONFLICT DO UPDATE SET ' + ', '.join(column_updates)
        else:
            raise Exception('unknown dialect: ' + str(dialect))
    else:
        raise Exception('invalid ON CONFLICT option: ' + str(on_conflict))

