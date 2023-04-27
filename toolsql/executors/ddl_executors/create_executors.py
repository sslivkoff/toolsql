from __future__ import annotations

from toolsql import dbs
from toolsql import drivers
from toolsql import spec
from toolsql import schemas
from toolsql import statements
from . import metadata_executors


def create_table(
    table: spec.TableSchema,
    *,
    if_not_exists: bool = False,
    conn: spec.Connection,
    table_only: bool = False,
    confirm: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify table')

    dialect = drivers.get_conn_dialect(conn)

    if table_only:
        sql = statements.build_create_table_statement(
            table=table,
            dialect=dialect,
            if_not_exists=if_not_exists,
        )
        conn.execute(sql)
    else:
        sqls = statements.build_all_table_schema_create_statements(
            table,
            dialect=dialect,
            if_not_exists=if_not_exists,
        )
        for sql in sqls:
            conn.execute(sql)


def create_db(
    *,
    db_schema: spec.DBSchema | None = None,
    db_config: spec.DBConfig,
    db_only: bool = False,
    if_not_exists: bool = False,
    confirm: bool = False,
    verbose: bool = False,
) -> None:

    if not confirm:
        raise Exception('must use confirm=True to modify database')

    # create database itself
    if not metadata_executors.does_db_exist(db_config):
        if verbose:
            print('creating', db_config['dbms'], 'database')
        db_class = dbs.get_db_class(db_config=db_config)
        db_class.create_db(db_config=db_config)
    else:
        if verbose:
            print(db_config['dbms'], 'database already exists')
        if not if_not_exists:
            raise Exception(db_config['dbms'] + ' database already exists')
    if db_only or db_schema is None:
        return

    # create database tables
    if isinstance(db_config, dict):
        dialect = db_config.get('dbms')
    elif isinstance(db_config, str):
        dialect = drivers.get_conn_dialect(db_config)
    else:
        raise Exception('invalid db_config format')
    db_schema = schemas.normalize_shorthand_db_schema(
        db_schema, dialect=dialect
    )
    tables = db_schema.get('tables')
    if tables is not None:
        with drivers.connect(db_config) as conn:
            for table in tables.values():
                create_table(
                    table=table,
                    conn=conn,
                    if_not_exists=if_not_exists,
                    confirm=confirm,
                )

