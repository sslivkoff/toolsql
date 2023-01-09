from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing_extensions import Literal
    from typing_extensions import TypedDict

    ColumnType = Literal[
        'BigInteger',
        'Binary',
        'Boolean',
        'Float',
        'IP',
        'Integer',
        'JSON',
        'Text',
        'Timestamp',
        'UUID',
    ]

    class DBSchema(TypedDict):
        tables: typing.Mapping[str, 'TableSpec']

    class TableSpec(TypedDict, total=False):
        name: str  # name of table, usually specified in DBSchema
        columns: typing.Sequence['ColumnSpec']
        constraints: typing.Sequence['TableConstraint']
        indices: typing.Sequence['TableIndex']
        column_order: typing.Sequence[str]

    class ColumnSpec(TypedDict, total=False):
        created_time: bool  # timestamp of creation time
        default: typing.Any  # default value
        fk_column: str  # column name
        fk_table: str  # table name
        index: bool  # whether to create an index for column
        inner_type: ColumnType  # for use by array
        length: int  # for use by Text columns
        modified_time: bool  # timestamp of creation time
        name: str  # name of table, usually specified in TableSpec
        null: bool  # for whether column can be null
        on_delete: 'DeleteOption'  # what to do when foreign key deleted
        primary: bool  # for use as primary key
        type: ColumnType
        unique: bool  # for creating unique index
        virtual: bool  # whether or not to actually create column in table

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

    class TableIndex(TypedDict, total=False):
        name: str
        columns: typing.Sequence[str]
        unique: bool
