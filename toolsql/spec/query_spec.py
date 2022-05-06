from __future__ import annotations

import typing
from typing_extensions import Literal
from typing_extensions import TypedDict

from . import sa_spec


class SelectQuery(TypedDict, total=False):
    table: str
    row_id: typing.Any
    row_ids: typing.Sequence[typing.Any]
    where_equals: typing.Mapping[str, typing.Any]  # column_name: column_value
    where_lte: typing.Mapping[str, typing.Any]  # column_name: column_value
    where_lt: typing.Mapping[str, typing.Any]  # column_name: column_value
    where_gte: typing.Mapping[str, typing.Any]  # column_name: column_value
    where_gt: typing.Mapping[str, typing.Any]  # column_name: column_value
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]]  # column_name
    where_foreign_row_equals: typing.Mapping[
        str,
        typing.Mapping[str, typing.Any],
    ]
    where_start_of: typing.Mapping[str, str]
    filters: typing.Sequence[sa_spec.SAStatement]
    filter_by: typing.Mapping[str, typing.Any]
    order_by: typing.Union['ColumnOrder', typing.Sequence['ColumnOrder']]
    only_columns: typing.Sequence[typing.Union[str, sa_spec.SAColumn]]


# return_count = of the results, how many to return, similar to LIMIT
# row_count = number of rows that can be in the result, otherwise raise error
ReturnCount = Literal['one', 'all']
RowCount = Literal['exactly_one', 'at_least_one', 'at_most_one']


#
# # ordering
#


class SQLColumnOrderMap(TypedDict):
    column: str
    order: Literal['descending', 'ascending']


SAColumnOrderObject = typing.Any

ColumnOrder = Literal[
    'Text',
    'ColumnOrderMap',
    'SAColumnOrderObject',
]


#
# # writes
#

ConflictOption = Literal['do_nothing', 'do_update']


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

