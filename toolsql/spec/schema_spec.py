from __future__ import annotations

import typing
from typing_extensions import Literal
from typing_extensions import TypedDict


ColumnType = Literal[
    'Boolean',
    'Integer',
    'BigInteger',
    'Float',
    'Text',
    'Map',
    'UUID',
    'Timestamp',
    'IP',
]


class DBSchema(TypedDict):
    tables: typing.Mapping[str, 'TableSpec']


class TableSpec(TypedDict):
    columns: typing.Mapping[str, 'ColumnSpec']
    constraints: typing.Sequence['TableConstraint']
    indices: typing.Seqeunce['TableIndex']
    column_order: typing.Sequence[str]


class ColumnSpec(TypedDict):
    type: ColumnType
    inner_type: ColumnType  # for use by array
    fk_table: str  # table name
    fk_column: str  # column name
    unique: bool  # for creating unique index
    primary: bool  # for use as primary key
    null: bool  # for whether column can be null
    created_time: bool  # timestamp of creation time
    modified_time: bool  # timestamp of creation time
    default: typing.Any  # default value
    length: int  # for use by Text columns
    virtual: bool  # whether or not to actually create column in table
    on_delete: 'DeleteOption'  # what to do when foreign key deleted
    index: bool  # whether to create an index for column


DeleteOption = Literal[
    'cascade',
    'set null',
    'set default',
    'restrict',
    'no action',
]


class TableConstraint(TypedDict):
    constrainttype: Literal['unique']
    columns: typing.Sequence[str]


class TableIndex(TypedDict):
    name: str
    columns: typing.Sequence[str]
    unique: bool

