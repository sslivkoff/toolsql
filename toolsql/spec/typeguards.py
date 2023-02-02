from __future__ import annotations

import typing

from . import typedata

if typing.TYPE_CHECKING:
    from typing_extensions import TypeGuard

    import pandas as pd
    import polars as pl

    from . import typedefs


def is_sqlite_columntype(
    item: typing.Any,
) -> TypeGuard[typedefs.SqliteColumntype]:
    return item in typedata.sqlite_columntypes


def is_postgresql_columntype(
    item: typing.Any,
) -> TypeGuard[typedefs.PostgresqlColumntype]:
    return item in typedata.postgresql_columntypes


def is_pandas_dataframe(
    item: typing.Any,
) -> TypeGuard[pd.DataFrame]:
    item_type = type(item)
    return (
        item_type.__name__ == 'DataFrame'
        and item_type.__module__ == 'pandas.core.frame'
    )


def is_polars_dataframe(
    item: typing.Any,
) -> TypeGuard[pl.DataFrame]:
    item_type = type(item)
    return (
        item_type.__name__ == 'DataFrame'
        and item_type.__module__ == 'polars.internals.dataframe.frame'
    )

