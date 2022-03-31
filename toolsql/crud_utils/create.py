from __future__ import annotations

import typing
from typing_extensions import Literal

from .. import spec
from .. import sqlalchemy_utils


NewRow = typing.Mapping[str, typing.Any]
MissingKeysOption = Literal['fill_in_copy', 'fill_in_inplace', 'chunk']


def insert(
    table: spec.TableRef,
    conn: spec.SAConnection,
    row: NewRow | None = None,
    rows: typing.Sequence[NewRow] | None = None,
    return_ids: bool | None = None,
    upsert: spec.ConflictOption | None = None,
    missing_keys: MissingKeysOption | None = None,
) -> typing.Any | None:

    if (row is None and rows is None) or (row is not None and rows is not None):
        raise Exception('specify either row or rows')

    if row is not None:
        return insert_row(
            row=row,
            table=table,
            conn=conn,
            upsert=upsert,
        )
    elif rows is not None:
        return insert_rows(
            rows=rows,
            table=table,
            conn=conn,
            return_ids=return_ids,
            upsert=upsert,
            missing_keys=missing_keys,
        )
    else:
        raise Exception('specify either row or rows')


def insert_row(
    row: NewRow,
    table: spec.TableRef,
    conn: spec.SAConnection,
    upsert: typing.Optional[spec.ConflictOption] = None,
) -> typing.Optional[typing.Any]:

    # create statement
    statement = create_insert_statement(
        table=table,
        conn=conn,
        row=row,
        upsert=upsert,
    )

    # execute statement
    # import sqlalchemy
    # result = typing.cast(
    #     sqlalchemy.engine.CursorResult,
    #     conn.execute(statement),
    # )
    result = conn.execute(statement)

    # process result
    primary_key = result.inserted_primary_key
    if upsert is not None and primary_key is None:
        return None
    elif len(primary_key) == 1:
        return primary_key[0]
    else:
        return primary_key


def insert_rows(
    rows: typing.Sequence[NewRow],
    table: spec.TableRef,
    conn: spec.SAConnection,
    return_ids: typing.Optional[bool] = None,
    upsert: typing.Optional[spec.ConflictOption] = None,
    missing_keys: typing.Optional[MissingKeysOption] = None,
) -> typing.Optional[typing.Sequence[typing.Any]]:

    if return_ids is None:
        return_ids = False

    # get table object
    if isinstance(table, str):
        table = sqlalchemy_utils.create_table_object_from_db(
            table_name=table, conn=conn
        )

    # insert rows
    if return_ids:
        # execute one-by-one if row_ids are needed
        ids = []
        for row in rows:
            inserted_id = insert_row(row=row, table=table, conn=conn)
            ids.append(inserted_id)
        return ids

    else:
        # execute in one statement if row_ids are not needed

        # compute row chunks
        if missing_keys is not None:
            row_chunks = process_missing_keys(
                rows=rows, missing_keys=missing_keys
            )
        else:
            row_chunks = [rows]

        # insert each row chunk
        statement = create_insert_statement(
            table=table, conn=conn, upsert=upsert
        )
        for row_chunk in row_chunks:
            conn.execute(statement, row_chunk)

        # not returning anything
        return None


def process_missing_keys(
    rows: typing.Sequence[NewRow],
    missing_keys: MissingKeysOption,
) -> typing.Sequence[typing.Sequence[NewRow]]:

    if missing_keys == 'fill_in_inplace':

        all_keys = set()
        for row in rows:
            all_keys.update(set(row.keys()))
        for row in rows:
            for key in all_keys:
                if key not in row:
                    row = dict(row, **{key: None})
        row_chunks = [rows]

    elif missing_keys == 'fill_in_copy':

        all_keys = set()
        for row in rows:
            all_keys.update(set(row.keys()))
        new_rows = []
        for row in rows:
            new_row = dict(row)
            for key in all_keys:
                if key not in row:
                    new_row[key] = None
            new_rows.append(new_row)
        rows = new_rows
        row_chunks = [rows]

    elif missing_keys == 'chunk':
        key_sets: dict[tuple[str, ...], list[NewRow]] = {}
        for row in rows:
            key_set = tuple(sorted(row.keys()))
            key_sets.setdefault(key_set, [])
            key_sets[key_set].append(row)
        row_chunks = list(key_sets.values())

    else:
        raise Exception('unknown missing_keys value: ' + str(missing_keys))

    return row_chunks


def create_insert_statement(
    table: spec.TableRef,
    conn: spec.SAConnection,
    row: typing.Mapping[str, typing.Any] | None = None,
    statement: spec.SAInsertStatement = None,
    upsert: spec.ConflictOption | None = None,
) -> spec.SAInsertStatement:

    # get table object
    if isinstance(table, str):
        table = sqlalchemy_utils.create_table_object_from_db(
            table_name=table, conn=conn
        )

    # create statement
    if statement is None:
        statement = sqlalchemy_utils.create_blank_insert_statement(
            table, conn=conn
        )

    # add row
    if row is not None:
        statement = statement.values(**row)

    if upsert is not None:
        primary_keys = [
            column.name for column in table.primary_key.columns.values()
        ]
        if upsert == 'do_nothing':
            statement = statement.on_conflict_do_nothing(
                index_elements=primary_keys
            )
        elif upsert == 'do_update':
            update_dict = {
                c.name: c for c in statement.excluded if not c.primary_key
            }
            statement = statement.on_conflict_do_update(
                index_elements=primary_keys,
                # set_=row,
                set_=update_dict,
            )
        else:
            raise Exception('unknown upsert value: ' + str(upsert))

    return statement

