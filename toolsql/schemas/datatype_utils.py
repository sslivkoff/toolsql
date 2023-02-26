from __future__ import annotations

import typing

from toolsql import spec


# these are lossy conversions
sqlite_to_postgres_conversions: typing.Mapping[
    spec.SqliteColumntype,
    spec.PostgresqlColumntype,
] = {
    'INTEGER': 'BIGINT',
    'FLOAT': 'DOUBLE PRECISION',
    'DECIMAL': 'NUMERIC',
    'TEXT': 'TEXT',
    'BLOB': 'BYTEA',
    'JSON': 'JSONB',
}

postgres_to_sqlite_conversions: typing.Mapping[
    spec.PostgresqlColumntype,
    spec.SqliteColumntype,
] = {
    'SMALLINT': 'INTEGER',
    'INT4': 'INTEGER',
    'BIGINT': 'INTEGER',
    'REAL': 'FLOAT',
    'DOUBLE PRECISION': 'FLOAT',
    'NUMERIC': 'DECIMAL',
    'TEXT': 'TEXT',
    'BYTEA': 'BLOB',
    'JSONB': 'JSON',
    'TIMESTAMPZ': 'INTEGER',
    'BOOLEAN': 'INTEGER',
}

generic_to_postgres_conversions: typing.Mapping[
    spec.GenericColumntype,
    spec.PostgresqlColumntype,
] = {
    'BINARY': 'BYTEA',
}

generic_to_sqlite_conversions: typing.Mapping[
    spec.GenericColumntype,
    spec.SqliteColumntype,
] = {
    'BINARY': 'BLOB',
}


def get_basic_python_types() -> typing.Mapping[
    spec.PythonColumntype, spec.Columntype
]:
    import decimal
    import datetime

    return {
        int: 'BIGINT',
        float: 'FLOAT',
        datetime.datetime: 'TIMESTAMPZ',
        decimal.Decimal: 'NUMERIC',
        str: 'TEXT',
        bytes: 'BINARY',
        dict: 'JSON',
        bool: 'BOOLEAN',
    }


def convert_columntype_to_dialect(
    columntype: spec.Columntype,
    dialect: spec.Dialect,
) -> spec.Columntype:
    if dialect == 'sqlite':
        return _convert_columntype_to_sqlite(columntype)
    elif dialect == 'postgresql':
        return _convert_columntype_to_postgres(columntype)
    else:
        raise Exception('unknown dialect: ' + str(dialect))


def _convert_columntype_to_sqlite(
    columntype: spec.Columntype,
) -> spec.SqliteColumntype:

    if isinstance(columntype, str):
        if columntype in spec.sqlite_columntypes:
            return columntype  # type: ignore
        elif columntype in postgres_to_sqlite_conversions:
            return postgres_to_sqlite_conversions[columntype]  # type: ignore
        elif columntype in generic_to_sqlite_conversions:
            return generic_to_sqlite_conversions[columntype]  # type: ignore
        else:
            raise Exception('unknown column type: ' + str(columntype))

    elif isinstance(columntype, type):
        basic_python_types = get_basic_python_types()
        if columntype in basic_python_types:
            return _convert_columntype_to_sqlite(basic_python_types[columntype])
        else:
            raise Exception('unknown column type: ' + str(columntype))

    else:
        raise Exception('unknown column type: ' + str(columntype))


def _convert_columntype_to_postgres(
    columntype: spec.Columntype,
) -> spec.PostgresqlColumntype:

    if isinstance(columntype, str):
        if columntype in spec.postgresql_columntypes:
            return columntype  # type: ignore
        elif columntype in sqlite_to_postgres_conversions:
            return sqlite_to_postgres_conversions[columntype]  # type: ignore
        elif columntype in generic_to_postgres_conversions:
            return generic_to_postgres_conversions[columntype]  # type: ignore
        else:
            raise Exception('unknown column type: ' + str(columntype))

    elif isinstance(columntype, type):
        basic_python_types = get_basic_python_types()
        if columntype in basic_python_types:
            return _convert_columntype_to_postgres(
                basic_python_types[columntype]
            )
        else:
            raise Exception('unknown column type: ' + str(columntype))

    else:
        raise Exception('unknown column type: ' + str(columntype))


def convert_table_schema_to_dialect(
    table_schema: spec.TableSchema, dialect: spec.Dialect
) -> spec.TableSchema:

    new_columns = []
    for column in table_schema['columns']:
        new_column = column.copy()
        new_column['type'] = convert_columntype_to_dialect(
            column['type'], dialect
        )
        new_columns.append(new_column)

    new_table_schema = table_schema.copy()
    new_table_schema['columns'] = new_columns

    return new_table_schema

