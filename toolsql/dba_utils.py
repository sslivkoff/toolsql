from __future__ import annotations

import os
import typing

import sqlalchemy  # type: ignore

from . import spec
from . import sqlalchemy_utils


def does_db_exist(db_config: spec.DBConfig) -> bool:
    if db_config['dbms'] == 'sqlite':
        return os.path.isfile(db_config['path'])
    else:
        raise NotImplementedError('dbms == ' + str(db_config['dbms']))


def create_table(table_name, table_schema, conn) -> None:
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy_utils.create_table_object_from_schema(
        table_name=table_name,
        table_schema=table_schema,
        metadata=metadata,
    )
    table.create(conn)

    # clear metadata cache once table is created
    clear_table_caches(conn)


def clear_table_caches(conn) -> None:
    """clear table caches after tables are created or dropped"""
    sqlalchemy_utils.metadata_utils._create_metadata_object_from_engine.cache.delete_all_entries()  # type: ignore
    db_url = conn.engine.url
    sqlalchemy_utils.table_utils._table_cache[db_url] = {}


def create_tables(
    *,
    spec_metadata: typing.Optional[spec.SAMetadata] = None,
    db_schema: typing.Optional[spec.DBSchema] = None,
    engine: typing.Optional[spec.SAEngine] = None,
    db_config: typing.Optional[spec.DBConfig] = None,
) -> None:
    if spec_metadata is None:
        if db_schema is None:
            raise Exception('must specify spec_metadata or db_schema')
        spec_metadata = sqlalchemy_utils.create_metadata_object_from_schema(
            db_schema=db_schema
        )
    if engine is None:
        if db_config is None:
            raise Exception('must specify engine or db_config')
        engine = sqlalchemy_utils.create_engine(db_config=db_config)
    print('creating tables...')
    spec_metadata.create_all(bind=engine)
    print('...all tables created')


def drop_tables(
    metadata: spec.SAMetadata,
    engine: spec.SAEngine,
    force: bool = False,
) -> None:
    if not force:
        raise Exception('use force=True')
    print('dropping tables...')
    metadata.drop_all(bind=engine)
    print('...all tables dropped')


def drop_all_rows(
    metadata: spec.SAMetadata,
    conn: typing.Optional[spec.SAConnection] = None,
    force: bool = False,
    engine: typing.Optional[spec.SAEngine] = None,
) -> None:
    if conn is None:
        if engine is None:
            raise Exception('specify conn or engine')
        conn = engine.connect()
    print('dropping rows...')
    with conn.begin():
        for table_name, table in metadata.tables.items():
            statement = metadata.tables[table_name].delete()
            conn.execute(statement)
    print('...all rows dropped')
