from __future__ import annotations

import typing
from typing_extensions import TypedDict

from . import schema_types


class TableDataset(TypedDict):
    schema: schema_types.TableSchema
    rows: typing.Sequence[tuple[typing.Any]]

