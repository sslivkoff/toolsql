from __future__ import annotations

import os
import typing

import toolsql
from toolsql import dbs
from toolsql import drivers
from toolsql import spec


def does_db_exist(db: str | spec.DBConfig) -> bool:
    if isinstance(db, str):
        db_config = drivers.parse_uri(db)
    elif isinstance(db, dict):
        db_config = db
    else:
        raise Exception('unknown db specification')

    if db_config.get('driver') not in ['sqlite3', 'psycopg']:
        if db_config['dbms'] == 'sqlite':
            db_config = dict(db_config, driver='sqlite3')  # type: ignore
        elif db_config['dbms'] == 'postgresql':
            db_config = dict(db_config, driver='psycopg')  # type: ignore
        else:
            raise Exception()

    if db_config['dbms'] == 'sqlite':
        path = db_config['path']
        if path is None:
            raise Exception('sqlite path not specified')
        return os.path.isfile(path)
    elif db_config['dbms'] == 'postgresql':
        try:
            with drivers.connect(db_config):
                pass
            return True
        except Exception:
            return False
    else:
        raise Exception('unknown dbms: ' + str(db_config['dbms']))


def get_db_schema(target: spec.Connection | spec.DBConfig) -> spec.DBSchema:
    if spec.is_sync_connection(target):
        conn = target
    elif isinstance(target, dict):
        with toolsql.connect(target) as conn:  # type: ignore
            return get_db_schema(target=conn)
    else:
        raise Exception('unknown target')

    name = drivers.get_conn_db_name(conn)
    table_schemas = get_table_schemas(conn=conn)
    return {
        'name': name,
        'description': None,
        'tables': table_schemas,
    }


def get_table_schemas(
    conn: spec.Connection,
) -> typing.Mapping[str, spec.TableSchema]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_schemas(conn)


def get_table_raw_column_types(
    table: str | spec.TableSchema, conn: spec.Connection | str | spec.DBConfig
) -> typing.Mapping[str, str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    result = db.get_table_raw_column_types(table=table, conn=conn)
    if len(result) == 0:
        if not has_table(table=table, conn=conn):  # type: ignore
            raise spec.TableDoesNotExist
    return result


async def async_get_table_raw_column_types(
    table: str | spec.TableSchema,
    conn: spec.AsyncConnection | str | spec.DBConfig,
) -> typing.Mapping[str, str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    result = await db.async_get_table_raw_column_types(table=table, conn=conn)
    if len(result) == 0:
        has_table = await async_has_table(table=table, conn=conn)  # type: ignore
        if not has_table:
            raise spec.TableDoesNotExist
    return result


def has_table(table: str | spec.TableSchema, conn: spec.Connection) -> bool:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.has_table(table=table, conn=conn)


async def async_has_table(
    table: str | spec.TableSchema, conn: spec.AsyncConnection
) -> bool:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return await db.async_has_table(table=table, conn=conn)


def get_table_names(
    conn: spec.Connection,
    *,
    permission: spec.TablePermission = 'read',
) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_names(conn=conn, permission=permission)


def get_indices_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_indices_names(conn=conn)


def get_table_schema(
    table: str | spec.TableSchema, conn: spec.Connection
) -> spec.TableSchema:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_schema(table=table, conn=conn)

