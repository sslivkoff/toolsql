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
    upsert: bool | None = None,
) -> tuple[str, spec.ExecuteManyParams]:
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

    if columns is None:
        first_row = rows[0]
        if isinstance(first_row, dict):
            columns = list(first_row.keys())
            if isinstance(table, dict):
                table_columns = {column['name'] for column in table['columns']}
                columns = [column for column in columns if column in table_columns]
            columns_expression = '(' + ','.join(columns) + ')'
        else:
            columns_expression = ''
    else:
        columns_expression = '(' + ','.join(columns) + ')'

    values_expression = _create_values_expression(
        rows=rows,
        columns=columns,
        dialect=dialect,
    )

    conflict_expression = _create_conflict_expression(
        on_conflict=on_conflict,
        upsert=upsert,
        dialect=dialect,
        columns=columns,
        rows=rows,
        table=table,
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
) -> spec.ExecuteManyParams:

    # convert to rows
    if row is not None and rows is not None:
        raise Exception()
    elif row is not None:
        return_rows: spec.ExecuteManyParams = [row]
    elif rows is not None:
        return_rows = rows
    else:
        raise Exception('must specify row or rows')

    # encode json columns
    return_rows = formats.encode_json_columns(rows=return_rows, dialect=dialect)

    return return_rows


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
                raise Exception('need columns specified')

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
    dialect: spec.Dialect,
    columns: typing.Sequence[str] | None,
    rows: spec.ExecuteManyParams | None,
    upsert: bool | None,
    table: str | spec.TableSchema,
) -> str:

    # dialect = 'postgresql'

    if upsert is not None:
        if on_conflict is not None:
            raise Exception('cannot specify both insert and on_conflict')
        if upsert:
            on_conflict = 'update'
        else:
            on_conflict = None

    if on_conflict is None:
        return ''
    elif on_conflict == 'ignore':
        if dialect == 'sqlite':
            return 'ON CONFLICT DO NOTHING'
        elif dialect == 'postgresql':
            return 'ON CONFLICT DO NOTHING'
        else:
            raise Exception('unknown dialect: ' + str(dialect))
    elif on_conflict == 'update':
        if dialect in ['sqlite', 'postgresql']:

            # build list of columns ot update
            if columns is None:
                if rows is None or len(rows) == 0:
                    return ''
                else:
                    if isinstance(rows[0], dict):
                        columns = list(rows[0].keys())
                    elif isinstance(table, dict):
                        columns = [column['name'] for column in table['columns']]
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
            column_updates_str = ', '.join(column_updates)

            # build constraint list
            if dialect == 'postgresql':
                if isinstance(table, dict):
                    unique_columns = [
                        column_schema['name']
                        for column_schema in table['columns']
                        if column_schema['primary'] or column_schema['unique']
                    ]
                else:
                    raise Exception(
                        'must explicitly specify columns for postgresql upserts'
                    )
                if len(unique_columns) == 0:
                    constraints = ''
                else:
                    constraints = '(' + ','.join(unique_columns) + ')'
            else:
                constraints = ''

            return 'ON CONFLICT {constraints} DO UPDATE SET {column_updates}'.format(
                constraints=constraints,
                column_updates=column_updates_str,
            )

        else:
            raise Exception('unknown dialect: ' + str(dialect))
    else:
        raise Exception('invalid ON CONFLICT option: ' + str(on_conflict))

