from __future__ import annotations

import typing

from toolsql import spec

if typing.TYPE_CHECKING:
    import polars as pl


def format_row_tuples(
    rows: typing.Sequence[tuple[typing.Any, ...]],
    output_format: spec.QueryOutputFormat | None = None,
    *,
    names: typing.Sequence[str] | None,
    output_dtypes: spec.OutputDtypes | None = None,
) -> spec.SelectOutputData:

    if output_format == 'tuple':
        return rows

    elif output_format in [
        'single_tuple',
        'single_tuple_or_none',
        'cell',
        'cell_or_none',
    ]:
        if len(rows) > 1:
            raise Exception('more than one row returned')
        elif len(rows) == 0:
            if output_format in [
                'single_tuple_or_none',
                'single_row_or_none',
                'cell_or_none',
            ]:
                return None
            else:
                raise Exception('no row found')
        else:
            if output_format in ['single_tuple', 'single_tuple_or_none']:
                return rows[0]
            elif output_format in ['cell', 'cell_or_none']:
                if len(rows[0]) == 0:
                    raise Exception('no column in row')
                elif len(rows[0]) == 1:
                    return rows[0][0]
                else:
                    raise Exception('too many columns in row')
            else:
                raise Exception('too many rows in result')

    elif output_format == 'single_column':
        if len(rows) == 0:
            return []
        else:
            if len(rows[0]) == 0:
                raise Exception('no columns returned')
            elif len(rows[0]) > 1:
                raise Exception('more than 1 column returned')
            else:
                return tuple(row[0] for row in rows)

    if names is None:
        if len(rows) == 0:
            output: spec.DictRows = []
            return output
        else:
            raise Exception('could not determine names of columns')

    if output_format == 'dict':
        return [dict(zip(names, row)) for row in rows]

    elif output_format in ['single_dict', 'single_dict_or_none']:
        if len(rows) > 1:
            raise Exception('too many rows in result')
        elif len(rows) == 0:
            if output_format == 'single_dict_or_none':
                return None
            else:
                raise Exception('no row in result')
        else:
            return dict(zip(names, rows[0]))

    elif output_format == 'polars':
        import polars as pl

        if output_dtypes is not None:
            dtypes = output_dtypes
        else:
            test_df = pl.DataFrame(rows[:100], schema=names)
            dtypes = [
                dtype if not isinstance(dtype, (pl.List, pl.Struct)) else pl.Object  # type: ignore
                for dtype in test_df.dtypes
            ]
        return pl.DataFrame(
            [tuple(row) for row in rows],
            schema=list(zip(names, dtypes)),
            orient='row',
        )

    elif output_format == 'pandas':
        import pandas as pd  # type: ignore

        # better way to convert rows to dataframes?
        return pd.DataFrame([tuple(row) for row in rows], columns=names)
    else:
        raise Exception('unknown output format: ' + str(output_format))


def format_row_dataframe(
    rows: pl.DataFrame, output_format: spec.QueryOutputFormat
) -> spec.SelectOutputData:

    # convert to output_format
    if output_format is None:
        output_format = 'polars'
    if output_format == 'polars':
        import polars as pl

        if isinstance(rows, pl.DataFrame):
            return rows
        else:
            raise Exception('improper format')
    elif output_format == 'pandas':
        import pandas as pd

        if isinstance(rows, pd.DataFrame):
            return rows
        else:
            raise Exception('improper format')
    elif output_format == 'tuple':
        # return list(zip(*rows.to_dict().values()))
        return rows.rows()
    elif output_format == 'dict':
        as_dicts: spec.DictRows = rows.to_dicts()
        return as_dicts
    elif output_format in [
        'single_tuple',
        'single_tuple_or_none',
        'single_dict',
        'single_dict_or_none',
        'cell',
        'cell_or_none',
    ]:
        if len(rows) == 0:
            if output_format in [
                'single_tuple_or_none',
                'single_dict_or_none',
                'cell_or_none',
            ]:
                return None
            else:
                raise Exception('not enough rows returned')
        elif len(rows) > 1:
            raise Exception('too many rows returned')
        else:
            if output_format in ['single_tuple', 'single_tuple_or_none']:
                return rows.row(0)
            elif output_format in ['single_dict', 'single_dict_or_none']:
                return rows.to_dicts()[0]
            elif output_format in ['cell', 'cell_or_none']:
                if len(rows.columns) != 1:
                    raise Exception('improper shape of output for cell')
                return rows.row(0)[0]
            else:
                raise Exception('unknown output format')
    elif output_format == 'single_column':
        if len(rows.columns) != 1:
            raise Exception('improper number of columns for single_column')
        else:
            return tuple(rows[rows.columns[0]])
    else:
        raise Exception('unknown output format: ' + str(output_format))

