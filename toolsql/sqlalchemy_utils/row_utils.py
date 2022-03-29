from __future__ import annotations

import typing

from .. import spec
from .. import exceptions
from .. import crud_utils
from . import table_utils


def row_to_dict(row: spec.SARow) -> dict[str, typing.Any]:
    return row._asdict()


def replace_row_foreign_keys(
    *,
    row: spec.Row,
    conn: spec.SAConnection,
    table: spec.TableRef,
    foreign_name: str | None = None,
    foreign_names: typing.Mapping[str, str] | None = None,
    insert_missing_rows: bool = False,
) -> dict[str, typing.Any]:

    if isinstance(table, str):
        table = table_utils.create_table_object_from_db(
            table_name=table, conn=conn
        )

    new_row = {}
    for column_name, column_value in row.items():
        column = table.c[column_name]
        if len(column.foreign_keys) == 1 and isinstance(column_value, str):
            if foreign_name is not None:
                name = foreign_name
            elif foreign_names is not None:
                name = foreign_names[column_name]
            else:
                raise Exception('must specify foreign_name or foreign_names')
            foreign_table = next(iter(column.foreign_keys)).column.table
            result = crud_utils.select(
                conn=conn,
                table=foreign_table,
                where_equals={name: column_value},
                include_id=True,
                row_count='at_most_one',
                return_count='one',
            )
            if result is None:
                if insert_missing_rows:
                    foreign_id = crud_utils.insert_row(
                        row={name: column_value},
                        table=foreign_table,
                        conn=conn,
                    )
                else:
                    raise exceptions.DoesNotExistException('row not found')
            else:
                foreign_id = list(result.keys())[0]
            new_row[column_name] = foreign_id
        else:
            new_row[column_name] = column_value

    return new_row

