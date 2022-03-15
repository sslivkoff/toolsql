from .. import sqlalchemy_utils


def insert(
    table,
    conn,
    row=None,
    rows=None,
    return_ids=None,
    upsert=None,
    missing_keys=None,
    **kwargs
):

    if (row is None and rows is None) or (row is not None and rows is not None):
        raise Exception('specify either row or rows')

    common_kwargs = dict(kwargs, table=table, conn=conn, upsert=upsert)
    if row is not None:
        return insert_row(row=row, **common_kwargs)
    elif rows is not None:
        return insert_rows(
            rows=rows,
            return_ids=return_ids,
            missing_keys=missing_keys,
            **common_kwargs
        )
    else:
        raise Exception('specify either row or rows')


def insert_row(row, table, conn, upsert=None):

    # create statement
    statement = create_insert_statement(
        table=table, conn=conn, row=row, upsert=upsert,
    )

    # execute statement
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
    rows, table, conn, return_ids=None, upsert=None, missing_keys=None
):

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


def process_missing_keys(rows, missing_keys):

    if missing_keys == 'fill_in_inplace':

        all_keys = set()
        for row in rows:
            all_keys.update(set(row.keys()))
        for row in rows:
            for key in all_keys:
                if key not in row:
                    row[key] = None
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
        key_sets = {}
        for row in rows:
            key_set = tuple(sorted(row.keys()))
            key_sets.setdefault(key_set, [])
            key_sets[key_set].append(row)
        row_chunks = list(key_sets.values())

    else:
        raise Exception('unknown missing_keys value: ' + str(missing_keys))

    return row_chunks


def create_insert_statement(table, conn, row=None, statement=None, upsert=None):

    # get table object
    if isinstance(table, str):
        table = sqlalchemy_utils.create_table_object_from_db(
            table_name=table, conn=conn
        )

    # create statement
    if statement is None:
        statement = sqlalchemy_utils.create_insert_statement(table)
        # statement = table.insert()

    # add row
    if row is not None:
        statement = statement.values(**row)

    if upsert is not None:
        if upsert == 'do_nothing':
            statement = statement.on_conflict_do_nothing()
        elif upsert == 'do_update':
            statement = statement.on_conflict_do_update(set_=row)
        else:
            raise Exception('unknown upsert value: ' + str(upsert))

    return statement

