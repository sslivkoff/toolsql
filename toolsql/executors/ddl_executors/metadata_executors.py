from __future__ import annotations

import os
import typing

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


def get_db_schema(conn: spec.Connection) -> spec.DBSchema:
    name = drivers.get_conn_db_name(conn)
    table_schemas = get_table_schemas(conn=conn)
    return {
        'name': name,
        'tables': table_schemas,
    }


def get_table_schemas(
    conn: spec.Connection,
) -> typing.Mapping[str, spec.TableSchema]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_schemas(conn=conn)


def get_table_raw_column_types(
    table: str | spec.TableSchema, conn: spec.Connection | str | spec.DBConfig
) -> typing.Mapping[str, str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_raw_column_types(table=table, conn=conn)


async def async_get_table_raw_column_types(
    table: str | spec.TableSchema,
    conn: spec.AsyncConnection | str | spec.DBConfig,
) -> typing.Mapping[str, str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return await db.async_get_table_raw_column_types(table=table, conn=conn)


def has_table(table: str | spec.TableSchema, conn: spec.Connection) -> bool:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.has_table(table=table, conn=conn)


def get_table_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_tables_names(conn=conn)


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

