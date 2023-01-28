import os
import tempfile


#
# # sqlite setup
#

tempdir = tempfile.mkdtemp()
test_sqlite_path = os.path.join(tempdir, 'test_db.sqlite')
sqlite_db_config = {
    'dbms': 'sqlite',
    'path': test_sqlite_path,
}


#
# # postgres setup
#

postgres_db_config = {
    'dbms': 'postgresql',
    'username': 'toolsql_test',
    'database': 'toolsql_test',
}


#
# # db_config's
#

# dbapi

sync_dbapi_db_configs = [
    {'driver': 'sqlite3', **sqlite_db_config},
    {'driver': 'psycopg', **postgres_db_config},
]

async_dbapi_db_configs = [
    {'driver': 'aiosqlite', **sqlite_db_config},
    {'driver': 'psycopg', **postgres_db_config},
]


# sync

sync_read_conn_db_configs = [
    {'driver': 'sqlite3', **sqlite_db_config},
    {'driver': 'psycopg', **postgres_db_config},
    {'driver': 'connectorx', **sqlite_db_config},
    {'driver': 'connectorx', **postgres_db_config},
]

sync_read_bare_db_configs = [
    {'driver': 'connectorx', **sqlite_db_config},
    {'driver': 'connectorx', **postgres_db_config},
]


# async

async_read_conn_db_configs = [
    {'driver': 'aiosqlite', **sqlite_db_config},
    {'driver': 'psycopg', **postgres_db_config},
    {'driver': 'connectorx', **sqlite_db_config},
    {'driver': 'connectorx', **postgres_db_config},
]

async_read_bare_db_configs = [
    {'driver': 'connectorx', **sqlite_db_config},
    {'driver': 'connectorx', **postgres_db_config},
]

#
# # write configs
#

sync_write_db_configs = [
    {'driver': 'sqlite3', **sqlite_db_config},
    {'driver': 'psycopg', **postgres_db_config},
]

async_write_db_configs = [
    {'driver': 'aiosqlite', **sqlite_db_config},
    {'driver': 'psycopg', **postgres_db_config},
]

