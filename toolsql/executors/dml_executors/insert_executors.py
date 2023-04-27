from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

from toolsql import drivers
from toolsql import spec
from toolsql import statements


def insert(
    *,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    conn: spec.Connection | spec.DBConfig,
    on_conflict: spec.OnConflictOption | None = None,
    upsert: bool | None = None,
    _use_postgresql_copy: bool = False,
) -> None:

    if _use_postgresql_copy:
        from toolsql.dbs.db_classes import postgresql_db

        return postgresql_db.PostgresqlDb._postgresql_copy(
            row=row,
            rows=rows,
            table=table,
            columns=columns,
            conn=conn,
            on_conflict=on_conflict,
            upsert=upsert,
        )

    if spec.is_polars_dataframe(rows):
        return _insert_polars(
            df=rows,
            table=table,
            conn=conn,
        )
    elif isinstance(conn, dict):
        raise Exception()

    # build insert statement
    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_insert_statement(
        row=row,
        rows=rows,
        table=table,
        columns=columns,
        dialect=dialect,
        on_conflict=on_conflict,
        upsert=upsert,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    driver.executemany(conn=conn, sql=sql, parameters=parameters)


def _insert_polars(
    df: pl.DataFrame,
    table: str | spec.TableSchema,
    uri: str | None = None,
    db_config: spec.DBConfig | None = None,
    conn: spec.Connection | spec.DBConfig | None = None,
) -> None:

    if uri is None:
        if db_config is None:
            if isinstance(conn, dict):
                db_config = conn
            else:
                raise Exception()
        uri = drivers.get_db_uri(db_config)

    if isinstance(table, str):
        table_name = table
    elif isinstance(table, dict):
        table_name = table['name']
    else:
        raise Exception()

    if uri.startswith('sqlite://'):
        uri = uri.replace('sqlite:', 'sqlite:/')
        # import os

        # path = uri.split('sqlite://')[1]
        # dirpath = os.path.dirname(path)
        # os.makedirs(dirpath, exist_ok=True)

    df.write_database(
        table_name=table_name,
        connection_uri=uri,
        if_exists='replace',
        engine='sqlalchemy',  # pip install adbc_driver_postgresql adbc_driver_sqlite
    )


async def async_insert(
    *,
    sql: str | None = None,
    row: spec.ExecuteParams | None = None,
    rows: spec.ExecuteManyParams | None = None,
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    conn: spec.AsyncConnection,
    on_conflict: spec.OnConflictOption | None = None,
    upsert: bool | None = None,
) -> None:

    # build insert statement
    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_insert_statement(
        row=row,
        rows=rows,
        table=table,
        columns=columns,
        dialect=dialect,
        on_conflict=on_conflict,
        upsert=upsert,
    )

    # execute query
    driver = drivers.get_driver_class(conn=conn)
    await driver.async_executemany(conn=conn, sql=sql, parameters=parameters)

