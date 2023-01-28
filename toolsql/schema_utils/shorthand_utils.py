from __future__ import annotations

import typing

from toolsql import spec
from . import datatype_utils


def normalize_shorthand_table_schema(
    table: spec.TableSchema | spec.TableSchemaShorthand,
) -> spec.TableSchema:

    columns = _normalize_shorthand_columns(table['columns'])
    constraints = table.get('constraints')
    if constraints is None:
        constraints = []

    indices = table.get('indices')
    if indices is None:
        indices = []

    return {
        'name': table['name'],
        'columns': columns,
        'constraints': constraints,
        'indices': indices,
    }


def _normalize_shorthand_columns(
    columns: spec.ColumnsShorthand,
) -> typing.Sequence[spec.ColumnSchema]:

    if isinstance(columns, (list, tuple)):
        columns = columns
    elif isinstance(columns, dict):
        new_columns: list[spec.ColumnSchemaShorthand] = []
        for name, column in columns.items():
            if isinstance(column, (str, type)):
                new_column: spec.ColumnSchemaPartial = {
                    'name': name,
                    'type': column,
                }
                new_columns.append(new_column)
            elif isinstance(column, dict):
                if 'name' in column:
                    if column['name'] != name:
                        raise Exception(
                            'conflicting name in table specification'
                        )
                    new_columns.append(column)  # type: ignore
                else:
                    new_columns.append(dict(column, name=name))  # type: ignore
            else:
                raise Exception('unknown column format: ' + str(type(column)))
        columns = new_columns  # type: ignore
    else:
        raise Exception('unknown columns format: ' + str(type(columns)))

    columns = [_normalize_shorthand_column(column) for column in columns]

    return columns


def _normalize_shorthand_column(
    column: spec.ColumnSchemaShorthand,
) -> spec.ColumnSchema:

    if not isinstance(column, dict):
        raise Exception('unknown column specification: ' + str(column))

    index = column.get('index')
    if index is None:
        index = False

    default = column.get('default')

    name = column.get('name')
    if name is None:
        raise Exception('column name not specified')

    primary = column.get('primary')
    if primary is None:
        primary = False

    nullable = column.get('nullable')
    if nullable is None:
        nullable = not primary

    column_type = column.get('type')
    if column_type is None:
        raise Exception('column type not specified')
    python_types = datatype_utils.get_basic_python_types()
    if not isinstance(column_type, str):
        if isinstance(column_type, type) and column_type in python_types:
            column_type = python_types[column_type]
        else:
            raise Exception('unknown column type: ' + str(column_type))
    if not spec.is_sqlite_columntype(
        column_type
    ) and not spec.is_postgresql_columntype(column_type):
        raise Exception('could not determine valid columntype')
    column_type = column_type.upper()

    unique = column.get('unique')
    if unique is None:
        unique = False

    return {
        'default': default,
        'index': index,
        'name': name,
        'nullable': nullable,
        'primary': primary,
        'type': column_type,
        'unique': unique,
    }

