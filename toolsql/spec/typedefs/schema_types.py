from __future__ import annotations

import typing

from typing_extensions import Literal
from typing_extensions import NotRequired
from typing_extensions import TypedDict


PythonColumntype = type

# https://www.sqlite.org/datatype3.html
SqliteColumntype = Literal[
    'INTEGER',
    'FLOAT',
    'DECIMAL',
    'TEXT',
    'BLOB',
    'JSON',
]

# https://www.postgresql.org/docs/current/datatype.html
PostgresqlColumntype = Literal[
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

GenericColumntype = Literal[
    'BINARY',  # converts to BLOB or BYTEA
]

ColumntypeShorthand = typing.Union[
    PythonColumntype,
    SqliteColumntype,
    PostgresqlColumntype,
    str,
    type,
]
Columntype = typing.Union[
    SqliteColumntype,
    PostgresqlColumntype,
    GenericColumntype,
]


class ColumnSchemaPartial(TypedDict, total=False):
    description: NotRequired[str | None]
    default: typing.Any  # default value
    index: bool  # whether to create an index for column
    name: str  # name of table, usually specified in TableSpec
    nullable: bool  # for whether column can be null
    primary: bool  # for use as primary key
    type: ColumntypeShorthand
    unique: bool  # for creating unique index
    autoincrement: Literal[False, True, 'strict']


class ColumnSchema(TypedDict):
    description: NotRequired[str | None]
    default: typing.Any  # default value
    index: bool  # whether to create an index for column
    name: str  # name of table, usually specified in TableSpec
    nullable: bool  # for whether column can be null
    primary: bool  # for use as primary key
    type: Columntype
    unique: bool  # for creating unique index
    autoincrement: Literal[False, True, 'strict']
    #
    # in near future
    # on_delete: DeleteOption  # what to do when foreign key deleted
    # fk_column: str  # column name
    # fk_table: str  # table name
    #
    # possible in future
    # length: int  # for use by Text columns
    # created_time: bool  # timestamp of creation time
    # modified_time: bool  # timestamp of creation time
    # inner_type: ColumnType  # for use by array
    # virtual: bool  # whether or not to actually create column in table


DeleteOption = Literal[
    'cascade',
    'set null',
    'set default',
    'restrict',
    'no action',
]


ColumnSchemaShorthand = typing.Union[
    ColumntypeShorthand,
    ColumnSchemaPartial,
    ColumnSchema,
]


class TableSchema(TypedDict):
    name: str
    description: NotRequired[str | None]
    columns: typing.Sequence[ColumnSchema]
    constraints: typing.Sequence[ConstraintSchema]
    indices: typing.Sequence[IndexSchema]


ColumnsShorthand = typing.Union[
    typing.Mapping[str, ColumnSchemaShorthand],
    typing.Sequence[ColumnSchemaShorthand],
]


class TableSchemaShorthand(TypedDict, total=False):
    name: str
    description: NotRequired[str | None]
    columns: ColumnsShorthand
    constraints: typing.Sequence[ConstraintSchema]
    indices: typing.Sequence[IndexSchema]


class ConstraintSchema(TypedDict):
    constrainttype: Literal['unique']
    columns: typing.Sequence[str]


class IndexSchema(TypedDict, total=False):
    name: str | None
    columns: typing.Sequence[str]
    unique: bool
    nulls_equal: bool


class DBSchema(TypedDict):
    name: str | None
    description: NotRequired[str | None]
    tables: typing.Mapping[str, TableSchema]
    # triggers: typing.Sequence[TriggerDefinition]
    # views: typing.Sequence[ViewDefinition]


class DBSchemaShorthand(TypedDict):
    name: NotRequired[str | None]
    description: NotRequired[str | None]
    tables: typing.Sequence[TableSchemaShorthand] | typing.Mapping[
        str, TableSchemaShorthand
    ]
    # triggers: typing.Sequence[TriggerDefinition]
    # views: typing.Sequence[ViewDefinition]


# TriggerDefinition = str
# ViewDefinition = str

