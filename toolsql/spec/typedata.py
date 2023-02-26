from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
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
