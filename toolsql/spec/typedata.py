from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl
    from . import typedefs


sqlite_columntypes: typing.Sequence[typedefs.SqliteColumntype] = [
    'INTEGER',
    'FLOAT',
    'DECIMAL',
    'TEXT',
    'BLOB',
    'JSON',
]

postgresql_columntypes: typing.Sequence[typedefs.PostgresqlColumntype] = [
    'SMALLINT',  # INT16
    'INT4',  # INT32
    'BIGINT',  # INT64
    'REAL',  # FLOAT32
    'DOUBLE PRECISION',  # FLOAT64
    'NUMERIC',  # DECIMAL
    'TEXT',
    'BYTEA',  # BINARY
    'JSONB',
    'TIMESTAMPZ',
    'BOOLEAN',
]

generic_columntypes: typing.Sequence[typedefs.GenericColumntype] = [
    'BINARY',
]


binary_columntypes = ['BLOB', 'BYTEA', 'BINARY']
integer_columntypes = ['INTEGER', 'SMALLINT', 'INT4', 'BIGINT']
float_columntypes = ['FLOAT', 'REAL', 'DOUBLE PRECISION']
decimal_columntypes = ['DECIMAL', 'NUMERIC']
text_columntypes = ['TEXT']
json_columntypes = ['JSON', 'JSONB']


@typing.overload
def columntype_to_polars_dtype(
    columntype: typedefs.Columntype,
    require: typing.Literal[True],
) -> pl.datatypes.DataTypeClass:
    ...


@typing.overload
def columntype_to_polars_dtype(
    columntype: typedefs.Columntype,
    require: bool = False,
) -> pl.datatypes.DataTypeClass | None:
    ...


def columntype_to_polars_dtype(
    columntype: typedefs.Columntype,
    require: bool = False,
) -> pl.datatypes.DataTypeClass | None:

    import polars as pl

    if columntype in binary_columntypes:
        return pl.datatypes.Binary
    elif columntype in integer_columntypes:
        return pl.datatypes.Int64
    elif columntype in float_columntypes:
        return pl.datatypes.Float64
    elif columntype in decimal_columntypes:
        return pl.datatypes.Decimal
    elif columntype in text_columntypes:
        return pl.datatypes.Utf8
    elif columntype in json_columntypes:
        return pl.datatypes.Object
    elif columntype == 'BOOLEAN':
        return pl.datatypes.Boolean
    else:
        if require:
            raise Exception('unknown dtype for columntype: ' + str(columntype))
        else:
            return None

