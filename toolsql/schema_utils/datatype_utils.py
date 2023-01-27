from __future__ import annotations

import typing
from toolsql import spec


# these are lossy conversions
sqlite_to_postgres_conversions: typing.Mapping[
    spec.SqliteColumntype,
    spec.PostgresqlColumntype,
] = {
    'integer': 'bigint',
    'float': 'double precision',
    'decimal': 'numeric',
    'text': 'text',
    'blob': 'bytea',
    'json': 'jsonb',
}

postgres_to_sqlite_conversions: typing.Mapping[
    spec.PostgresqlColumntype,
    spec.SqliteColumntype,
] = {
    'smallint': 'integer',
    'int4': 'integer',
    'bigint': 'integer',
    'real': 'float',
    'double precision': 'float',
    'numeric': 'decimal',
    'text': 'text',
    'bytea': 'blob',
    'jsonb': 'json',
    'timestampz': 'integer',
}


def get_basic_python_types() -> typing.Mapping[
    spec.PythonColumntype, spec.Columntype
]:
    import decimal
    import datetime

    return {
        int: 'integer',
        float: 'float',
        datetime.datetime: 'timestampz',
        decimal.Decimal: 'numeric',
        str: 'text',
        bytes: 'blob',
        dict: 'json',
    }


def convert_columntype_to_dialect(
    columntype: spec.Columntype, dialect: spec.Dialect,
) -> spec.Columntype:
    if dialect == 'sqlite':
        return convert_columntype_to_sqlite(columntype)
    elif dialect == 'postgresql':
        return convert_columntype_to_postgres(columntype)
    else:
        raise Exception('unknown dialect: ' + str(dialect))


def convert_columntype_to_sqlite(
    columntype: spec.Columntype,
) -> spec.SqliteColumntype:

    if isinstance(columntype, str):
        if columntype in sqlite_to_postgres_conversions:
            return columntype  # type: ignore
        elif columntype in postgres_to_sqlite_conversions:
            return postgres_to_sqlite_conversions[columntype]  # type: ignore
        else:
            raise Exception('unknown column type: ' + str(columntype))

    elif isinstance(columntype, type):
        basic_python_types = get_basic_python_types()
        if columntype in basic_python_types:
            return convert_columntype_to_sqlite(
                basic_python_types[columntype]
            )
        else:
            raise Exception('unknown column type: ' + str(columntype))

    else:
        raise Exception('unknown column type: ' + str(columntype))


def convert_columntype_to_postgres(
    columntype: spec.Columntype,
) -> spec.PostgresqlColumntype:

    if isinstance(columntype, str):
        if columntype in sqlite_to_postgres_conversions:
            return sqlite_to_postgres_conversions[columntype]  # type: ignore
        elif columntype in postgres_to_sqlite_conversions:
            return columntype  # type: ignore
        else:
            raise Exception('unknown column type: ' + str(columntype))

    elif isinstance(columntype, type):
        basic_python_types = get_basic_python_types()
        if columntype in basic_python_types:
            sqlite_type = basic_python_types[columntype]
            return sqlite_to_postgres_conversions[sqlite_type]
        else:
            raise Exception('unknown column type: ' + str(columntype))

    else:
        raise Exception('unknown column type: ' + str(columntype))

