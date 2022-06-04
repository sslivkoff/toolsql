from __future__ import annotations

import typing

from . import crud_utils
from . import sqlalchemy_utils

if typing.TYPE_CHECKING:
    import pandas as pd

    from . import spec


def table_to_dataframe(
    table: str | spec.SATable,
    engine: spec.SAEngine | None = None,
    conn: spec.SAConnection | None = None,
    primary_key_index: bool = True,
) -> pd.DataFrame:

    import pandas as pd

    # get rows
    if conn is not None:
        rows = crud_utils.select(conn=conn, table=table)
    elif engine is not None:
        with engine.begin() as conn:
            rows = crud_utils.select(conn=conn, table=table)
    else:
        raise Exception('must specify conn or engine')

    # create dataframe
    df = pd.DataFrame(rows)

    # use primary keys as dataframe index
    if primary_key_index:

        # collect primary keys (turn into separate function)
        if isinstance(table, str):
            table_obj = sqlalchemy_utils.create_table_object_from_db(
                table_name=table, engine=engine
            )
        else:
            table_obj = table
        primary_keys = []
        for column in table_obj.primary_key.columns.values():
            primary_keys.append(column.name)

        # set index
        if len(primary_keys) == 0:
            raise Exception('no primary key detected')
        elif len(primary_keys) == 1:
            df = df.set_index(primary_keys[0])
        else:
            df = df.set_index(primary_keys)

    return df
