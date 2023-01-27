from __future__ import annotations

import decimal
import typing

from typing_extensions import Literal
from typing_extensions import TypedDict


# BasicTypesPython = Literal[
#     int,
#     float,
#     decimal.Decimal,
#     str,
#     bytes,
#     dict,
# ]
BasicTypesPython = type

BasicTypesText = Literal[
    'Integer',
    'Float',
    'Decimal',
    'Text',
    'Bytes',
    'JSON',
]

ColumnType = typing.Union[
    BasicTypesPython,
]


class ColumnSchema(TypedDict, total=False):
    default: typing.Any  # default value
    index: bool  # whether to create an index for column
    name: str  # name of table, usually specified in TableSpec
    nullable: bool  # for whether column can be null
    primary: bool  # for use as primary key
    type: ColumnType
    unique: bool  # for creating unique index
    #
    # in near future
    on_delete: DeleteOption  # what to do when foreign key deleted
    fk_column: str  # column name
    fk_table: str  # table name
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
    ColumnType,
    ColumnSchema,
]


class TableSchema(TypedDict):
    name: str
    columns: typing.Sequence[ColumnSchema]
    constraints: typing.Sequence[ConstraintSchema]
    indices: typing.Sequence[IndexSchema]


class TableSchemaShorthand(TypedDict, total=False):
    name: str
    columnns: typing.Mapping[str, ColumnSchema] | typing.Sequence[ColumnSchema]
    constraints: typing.Sequence[ConstraintSchema]
    indices: typing.Sequence[IndexSchema]


class ConstraintSchema(TypedDict):
    constrainttype: Literal['unique']
    columns: typing.Sequence[str]


class IndexSchema(TypedDict, total=False):
    name: str
    columns: typing.Sequence[str]
    unique: bool


class DatabaseSchema(TypedDict):
    tables: typing.Sequence[TableSchema]
    # triggers: typing.Sequence[TriggerDefinition]
    # views: typing.Sequence[ViewDefinition]


# TriggerDefinition = str
# ViewDefinition = str

