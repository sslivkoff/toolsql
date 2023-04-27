from __future__ import annotations

import typing

from toolsql import statements
from toolsql import spec
from . import datatype_utils


def normalize_shorthand_db_schema(
    db_schema: spec.DBSchema | spec.DBSchemaShorthand,
    *,
    dialect: spec.Dialect | None = None,
) -> spec.DBSchema:
    tables = db_schema['tables']
    if isinstance(tables, (list, tuple)):
        normalized_tables = {
            table['name']: normalize_shorthand_table_schema(
                table, dialect=dialect
            )
            for table in tables
        }
    elif isinstance(tables, dict):
        normalized_tables = {
            name: normalize_shorthand_table_schema(
                dict(table, name=name),  # type: ignore
                dialect=dialect,
            )
            for name, table in tables.items()
        }
    else:
        raise Exception('unknown format for tables: ' + str(tables))
    return {
        'name': db_schema.get('name'),
        'description': db_schema.get('description'),
        'tables': normalized_tables,
    }


def normalize_shorthand_table_schema(
    table: spec.TableSchema | spec.TableSchemaShorthand,
    *,
    dialect: spec.Dialect | None = None,
) -> spec.TableSchema:

    columns = normalize_shorthand_column_schemas(table['columns'], dialect=dialect)
    constraints = table.get('constraints')
    if constraints is None:
        constraints = []

    indices = table.get('indices')
    if indices is None:
        indices = []
    new_indices = []
    for index in indices:
        index = index.copy()
        if index.get('unique') is None:
            index['unique'] = False
        if index.get('nulls_equal') is None:
            index['nulls_equal'] = len(index['columns']) > 1
        if 'name' not in index:
            index['name'] = statements.create_default_index_name(
                index=index,
                table=table['name'],
            )
        new_indices.append(index)
    indices = new_indices

    return {
        'name': table['name'],
        'description': table.get('description'),
        'columns': columns,
        'constraints': constraints,
        'indices': indices,
    }


def normalize_shorthand_column_schemas(
    columns: spec.ColumnsShorthand,
    *,
    dialect: spec.Dialect | None = None,
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
        columns = new_columns
    else:
        raise Exception('unknown columns format: ' + str(type(columns)))

    return [
        normalize_shorthand_column_schema(column, dialect=dialect)
        for column in columns
    ]


def normalize_shorthand_column_schema(
    column: spec.ColumnSchemaShorthand,
    *,
    dialect: spec.Dialect | None = None,
) -> spec.ColumnSchema:

    if not isinstance(column, dict):
        raise Exception('unknown column specification: ' + str(column))

    default = column.get('default')

    name = column.get('name')
    if name is None:
        raise Exception('column name not specified')

    primary = column.get('primary')
    if primary is None:
        primary = False

    autoincrement = column.get('autoincrement')
    if autoincrement is None:
        autoincrement = False

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
    column_type = column_type.upper()
    if (
        not spec.is_sqlite_columntype(column_type)
        and not spec.is_postgresql_columntype(column_type)
        and not spec.is_generic_columntype(column_type)
    ):
        raise Exception('could not determine valid columntype')

    if dialect is not None:
        column_type = datatype_utils.convert_columntype_to_dialect(
            column_type, dialect=dialect
        )

    unique = column.get('unique')
    if unique is None:
        unique = False

    index = column.get('index')
    if index is None:
        index = unique or primary

    return {
        'default': default,
        'index': index,
        'name': name,
        'nullable': nullable,
        'primary': primary,
        'type': column_type,
        'unique': unique,
        'autoincrement': autoincrement,
        'description': column.get('description'),
    }

