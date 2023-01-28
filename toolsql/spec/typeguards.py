from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from . import typedefs

from typing_extensions import TypeGuard
from . import typedata


def is_sqlite_columntype(
    item: typing.Any,
) -> TypeGuard[typedefs.SqliteColumntype]:
    return item in typedata.sqlite_columntypes


def is_postgresql_columntype(
    item: typing.Any,
) -> TypeGuard[typedefs.PostgresqlColumntype]:
    return item in typedata.postgresql_columntypes

