from __future__ import annotations

import os
import typing

from toolsql import drivers
from toolsql import spec
from toolsql import statements
from . import dml_executors


def does_db_exist(db: str | spec.DBConfig) -> bool:

    if isinstance(db, str):
        db_config = drivers.parse_uri(db)
    elif isinstance(db, dict):
        db_config = db
    else:
        raise Exception('unknown db specification')

    if db_config['driver'] == 'connectorx':
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


#
# # row counts
#


def get_table_row_count(
    table: str | spec.TableSchema,
    *,
    conn: spec.Connection,
) -> int:

    result: int = dml_executors.select(
        table=table,
        conn=conn,
        columns=['COUNT(*)'],
        output_format='cell',
    )
    return result


async def async_get_table_row_count(
    table: str | spec.TableSchema,
    *,
    conn: spec.AsyncConnection,
) -> int:

    result: int = await dml_executors.async_select(
        table=table,
        conn=conn,
        columns=['COUNT(*)'],
        output_format='cell',
    )
    return result


#
# # byte counts
#


def get_table_nbytes(
    table: str | spec.TableSchema,
    *,
    conn: spec.Connection,
) -> int:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_table_nbytes_statement(table, dialect=dialect)
    result: int = dml_executors.raw_select(
        sql=sql, conn=conn, output_format='cell'
    )
    return result


async def async_get_table_nbytes(
    table: str | spec.TableSchema,
    *,
    conn: spec.AsyncConnection,
) -> int:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_table_nbytes_statement(table, dialect=dialect)
    result: int = await dml_executors.async_raw_select(
        sql=sql, conn=conn, output_format='cell'
    )
    return result


def get_tables_nbytes(*, conn: spec.Connection) -> typing.Mapping[str, int]:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_tables_nbytes_statement(dialect=dialect)
    result = dml_executors.raw_select(sql=sql, conn=conn, output_format='tuple')
    return dict(result)  # type: ignore


async def async_get_tables_nbytes(
    *, conn: spec.AsyncConnection
) -> typing.Mapping[str, int]:

    dialect = drivers.get_conn_dialect(conn=conn)
    sql = statements.build_get_tables_nbytes_statement(dialect=dialect)
    result = await dml_executors.async_raw_select(
        sql=sql, conn=conn, output_format='tuple'
    )
    return dict(result)  # type: ignore

