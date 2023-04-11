from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import TypeVar

    import pandas as pd  # type: ignore
    import polars as pl

    R = TypeVar(
        'R',
        typing.Sequence[tuple[typing.Any, ...]],
        pl.DataFrame,
        pd.DataFrame,
    )

from toolsql import spec


def decode_columns(
    *,
    rows: R,
    columns: spec.DecodeColumns | None = None,
) -> R:

    if columns is None:
        return rows

    # gather decoders
    column_decoders: typing.MutableSequence[
        typing.Callable[..., typing.Any] | None
    ] = []
    for column in columns:
        if column == 'JSON':
            import json

            column_decoders.append(json.loads)
        elif column == 'BOOLEAN':
            column_decoders.append(bool)
        elif column == 'INTEGER':
            column_decoders.append(int)
        elif column is None:
            column_decoders.append(None)
        else:
            raise Exception('unknown decoding type: ' + str(column))

    # decode based on row type
    if all(item is None for item in column_decoders):
        return rows

    elif isinstance(rows, (tuple, list)):
        return _decode_columns_sequence(
            rows=rows,
            column_decoders=column_decoders,
        )

    elif spec.is_polars_dataframe(rows):

        import polars as pl

        pl_types: typing.MutableSequence[pl.datatypes.DataTypeClass | None] = []
        for column in columns:
            if column == 'JSON':
                pl_types.append(pl.datatypes.Object)
            elif column == 'BOOLEAN':
                pl_types.append(pl.datatypes.Boolean)
            elif column == 'INTEGER':
                pl_types.append(pl.datatypes.Int64)
            elif column is None:
                pl_types.append(None)
            else:
                raise Exception('unknown decoding type: ' + str(column))

        return _decode_columns_polars(  # type: ignore
            rows=rows,
            column_decoders=column_decoders,
            pl_types=pl_types,
        )

    elif spec.is_pandas_dataframe(rows):
        return _decode_columns_pandas(  # type: ignore
            rows=rows,
            column_decoders=column_decoders,
        )

    else:
        raise Exception('invalid rows format: ' + str(type(rows)))


def _decode_columns_sequence(
    *,
    rows: typing.Sequence[tuple[typing.Any, ...]],
    column_decoders: typing.Sequence[None | typing.Callable[..., typing.Any]],
) -> typing.Sequence[tuple[typing.Any, ...]]:

    return [
        tuple(
            (decoder(cell) if decoder is not None else cell)
            for cell, decoder in zip(row, column_decoders)
        )
        for row in rows
    ]


def _decode_columns_polars(
    *,
    rows: pl.DataFrame,
    column_decoders: typing.Sequence[None | typing.Callable[..., typing.Any]],
    pl_types: typing.Sequence[pl.datatypes.DataTypeClass | None],
) -> pl.DataFrame:

    import polars as pl

    for c, decoder in enumerate(column_decoders):
        if decoder is None:
            continue
        column_name = rows.columns[c]
        pl_type = pl_types[c]
        if pl_type is None:
            raise Exception()

        if len(rows) == 0:
            decoded = pl.Series(name=column_name, dtype=pl_type)
        # elif rows[column_name].null_count() == 0:
        #     decoded = rows[column_name].apply(decoder, return_dtype=pl_type)
        else:

            decoded = pl.Series(
                column_name,
                [
                    decoder(item) if item not in (None, '') else None
                    for item in rows[column_name].to_list()
                ],
                dtype=pl_type,
            )
        rows = rows.with_columns(decoded)

    return rows


def _decode_columns_pandas(
    *,
    rows: pd.DataFrame,
    column_decoders: typing.Sequence[None | typing.Callable[..., typing.Any]],
) -> pd.DataFrame:

    for c, decoder in enumerate(column_decoders):
        if decoder is not None:
            column_name = rows.columns[c]
            rows[column_name] = rows[column_name].map(decoder)

    return rows

