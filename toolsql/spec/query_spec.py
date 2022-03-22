from __future__ import annotations

import typing
from typing_extensions import Literal
from typing_extensions import TypedDict

from . import sa_spec


class SQLQueryFilter(TypedDict):
    row_id: typing.Any
    row_ids: typing.Sequence[typing.Any]
    where_equals: typing.Mapping[str, typing.Any]  # column_name: column_value
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]]  # column_name
    where_foreign_row_equals: typing.Mapping[
        str,
        typing.Mapping[str, typing.Any],
    ]
    where_start_of: typing.Mapping[str, str]
    filters: typing.Sequence['SQLAlchemyStatementObject']
    filter_by: typing.Mapping[str, typing.Any]
    order_by: typing.Union['ColumnOrder', typing.Sequence['ColumnOrder']]
    only_columns: typing.Sequence[typing.Union[str, sa_spec.SAColumn]]


SQLAlchemyStatementObject = typing.Any
SQLReturnCount = typing.Literal['one', 'all']
SQLRowCount = typing.Literal['exactly_one', 'at_least_one', 'at_most_one']


#
# # ordering
#


class SQLColumnOrderMap(TypedDict):
    column: str
    order: typing.Literal['descending', 'ascending']


SAColumnOrderObject = typing.Any

ColumnOrder = Literal[
    'Text',
    'ColumnOrderMap',
    'SAColumnOrderObject',
]


#
# # filtering
#

# 'SQLFilterMap': {
#     # this is a WIP
#     # would be nice if there were a straightforward way to make it nested
#     # - e.g. with or's and and's
#     'operator': {'<=', '<', '==', '>', '>='},
#     'lhs': 'SQLColumnName',
#     'rhs': 'SQLColumnName',
# },

