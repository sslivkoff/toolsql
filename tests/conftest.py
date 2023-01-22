import asyncio
import os
import sqlite3
import tempfile

import psycopg
import pytest
import polars as pl
import pandas as pd

from toolsql.driver_utils.drivers.psycopg_driver import PsycopgDriver


@pytest.fixture(scope="session")
def event_loop(request):
    """create an instance of the default event loop for each test case

    adapted from https://stackoverflow.com/a/66225169
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


tempdir = tempfile.mkdtemp()
test_sqlite_path = os.path.join(tempdir, 'test_db.sqlite')
sqlite_db_config = {
    'dbms': 'sqlite',
    'path': test_sqlite_path,
}
postgres_db_config = {
    'dbms': 'postgresql',
    'username': 'toolsql_test',
    'database': 'toolsql_test',
}

sync_db_configs = [
    {'driver': 'sqlite3', **sqlite_db_config},
    {'driver': 'psycopg', **postgres_db_config},
    {'driver': 'connectorx', **sqlite_db_config},
    {'driver': 'connectorx', **postgres_db_config},
]

async_db_configs = [
    {'driver': 'aiosqlite', **sqlite_db_config},
    {'driver': 'asyncpg', **postgres_db_config},
    {'driver': 'connectorx', **sqlite_db_config},
    {'driver': 'connectorx', **postgres_db_config},
]


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

simple_columns = simple_table['columns'].keys()
raw_test_queries = {
    'SELECT * FROM simple_table': {
        'tuple': simple_table['rows'],
        'dict': [
            dict(zip(simple_columns, datum)) for datum in simple_table['rows']
        ],
        'polars': pl.DataFrame(simple_table['rows'], columns=simple_columns),
        'pandas': pd.DataFrame(simple_table['rows'], columns=simple_columns),
    },
}
select_queries = [
    {'sql': sql, 'output_format': output_format, 'target_result': target_result}
    for sql in raw_test_queries.keys()
    for output_format, target_result in raw_test_queries[sql].items()
]


@pytest.fixture(scope='session', autouse=True)
def setup_teardown():

    # setup sqlite tables
    with sqlite3.connect(test_sqlite_path) as conn:
        for test_table in test_tables.values():

            # drop table if exists
            conn.execute(test_table['drop'])

            # create table
            conn.execute(test_table['create'])

            # insert rows
            cursor = conn.cursor()
            cursor.executemany(
                'INSERT INTO {table_name} VALUES (?,?)'.format(
                    table_name=test_table['name']
                ),
                test_table['rows'],
            )

    # setup postgres tables
    db_config = dict(postgres_db_config, driver='psycopg')
    conn_str = PsycopgDriver.get_psycopg_conn_str(db_config)
    with psycopg.connect(conn_str) as conn:
        for test_table in test_tables.values():

            # drop table if exists
            conn.execute(test_table['drop'])

            # create table
            conn.execute(test_table['create'])

            # insert rows
            with conn.cursor() as cursor:
                cursor.executemany(
                    'INSERT INTO {table_name} VALUES (%s,%s)'.format(
                        table_name=test_table['name']
                    ),
                    test_table['rows'],
                )

    # transition to teardown
    yield

    # teardown
    pass


@pytest.fixture(name='sync_db_config', params=sync_db_configs, scope='session')
def _sync_db_config(request):
    return request.param


@pytest.fixture(
    name='async_db_config', params=async_db_configs, scope='session'
)
def _async_db_config(request):
    return request.param


@pytest.fixture(name='select_query', params=select_queries, scope='session')
def _select_queries(request):
    return request.param

