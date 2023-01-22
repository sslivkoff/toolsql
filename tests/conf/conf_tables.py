simple_table = {
    'name': 'simple_table',
    'columns': {'id': int, 'name': str},
    'drop': 'DROP TABLE IF EXISTS simple_table',
    'create': 'CREATE TABLE simple_table (id INTEGER PRIMARY KEY, name TEXT);',
    # 'clear': 'DELETE FROM simple_table',
    'rows': [
        (5, 'this'),
        (6, 'is'),
        (7, 'a'),
        (8, 'test'),
    ],
}
test_tables = {'simple_table': simple_table}


def insert_sqlite_table(table, conn):

    # drop table if exists
    conn.execute(table['drop'])

    # create table
    conn.execute(table['create'])

    # insert rows
    cursor = conn.cursor()
    cursor.executemany(
        'INSERT INTO {table_name} VALUES (?,?)'.format(
            table_name=table['name']
        ),
        table['rows'],
    )


def insert_postgres_table(table, conn):

    # drop table if exists
    conn.execute(table['drop'])

    # create table
    conn.execute(table['create'])

    # insert rows
    with conn.cursor() as cursor:
        cursor.executemany(
            'INSERT INTO {table_name} VALUES (%s,%s)'.format(
                table_name=table['name']
            ),
            table['rows'],
        )

