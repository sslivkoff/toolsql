import asyncio
import sqlite3
import uuid

import psycopg
import pytest

import toolsql
from toolsql.drivers.driver_classes.psycopg_driver import PsycopgDriver

import conf.conf_db_configs as conf_db_configs
import conf.conf_helpers as conf_helpers
import conf.conf_write_queries as conf_write_queries
import conf.conf_tables as conf_tables


@pytest.fixture(scope="session")
def event_loop(request):
    """create an instance of the default event loop for each test case

    adapted from https://stackoverflow.com/a/66225169
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


#
# # db configs
#

# dbapi


@pytest.fixture(params=conf_db_configs.sync_dbapi_db_configs)
def sync_dbapi_db_config(request):
    return request.param


@pytest.fixture(params=conf_db_configs.async_dbapi_db_configs)
def async_dbapi_db_config(request):
    return request.param


# sync read


@pytest.fixture(params=conf_db_configs.sync_read_conn_db_configs)
def sync_read_conn_db_config(request):
    return request.param


@pytest.fixture(params=conf_db_configs.sync_read_bare_db_configs)
def sync_read_bare_db_config(request):
    return request.param


# async read


@pytest.fixture(params=conf_db_configs.async_read_conn_db_configs)
def async_read_conn_db_config(request):
    return request.param


@pytest.fixture(params=conf_db_configs.async_read_bare_db_configs)
def async_read_bare_db_config(request):
    return request.param


# write


@pytest.fixture(params=conf_db_configs.sync_write_db_configs)
def sync_write_db_config(request):
    return request.param


@pytest.fixture(params=conf_db_configs.async_write_db_configs)
def async_write_db_config(request):
    return request.param


#
# # table data
#


@pytest.fixture(scope='session', autouse=True)
def setup_teardown():

    db_config = dict(conf_db_configs.postgres_db_config, driver='psycopg')
    conn_str = PsycopgDriver.get_psycopg_conn_str(db_config)

    # drop previously existing tables
    with sqlite3.connect(conf_db_configs.test_sqlite_path) as conn:
        for table_name in toolsql.get_table_names(conn):
            toolsql.drop_table(table=table_name, conn=conn, confirm=True)
    with psycopg.connect(conn_str) as conn:
        for table_name in toolsql.get_table_names(conn):
            toolsql.drop_table(table=table_name, conn=conn, confirm=True)

    # load table schemas
    test_tables = conf_tables.get_test_tables()
    for table_name in test_tables.keys():
        test_tables[table_name][
            'schema'
        ] = toolsql.normalize_shorthand_table_schema(
            test_tables[table_name]['schema']
        )

    # setup sqlite tables
    with sqlite3.connect(conf_db_configs.test_sqlite_path) as conn:
        for table_name, table in test_tables.items():
            toolsql.create_table(table=table['schema'], conn=conn, confirm=True)
    with sqlite3.connect(conf_db_configs.test_sqlite_path) as conn:
        for table_name, table in test_tables.items():
            toolsql.insert(rows=table['rows'], table=table_name, conn=conn)

    # setup postgres tables
    with psycopg.connect(conn_str) as conn:
        for table_name, table in test_tables.items():
            toolsql.create_table(table=table['schema'], conn=conn, confirm=True)
    with psycopg.connect(conn_str) as conn:
        for table_name, table in test_tables.items():
            toolsql.insert(rows=table['rows'], table=table_name, conn=conn)

    # transition to teardown
    yield

    # teardown
    pass


@pytest.fixture()
def fresh_simple_table():
    table = conf_tables.get_simple_table()
    table['schema']['name'] = 'simple_' + str(uuid.uuid4()).replace('-', '_')
    table['schema'] = toolsql.normalize_shorthand_table_schema(table['schema'])
    return table


@pytest.fixture()
def fresh_pokemon_table():
    table = conf_tables.get_pokemon_table()
    table['schema']['name'] = 'pokemon_' + str(uuid.uuid4()).replace('-', '_')
    table['schema'] = toolsql.normalize_shorthand_table_schema(table['schema'])
    return table


#
# # queries
#


@pytest.fixture(params=conf_write_queries.insert_queries)
def insert_queries(request):
    return request.param


@pytest.fixture(params=conf_write_queries.update_queries)
def update_queries(request):
    return request.param


@pytest.fixture(params=conf_write_queries.delete_queries)
def delete_query(request):
    return request.param


#
# # helpers
#


@pytest.fixture()
def helpers():
    return conf_helpers.ToolsqlTestHelpers

