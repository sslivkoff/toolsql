from __future__ import annotations

import typing

from . import spec
from . import sqlalchemy_utils


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

