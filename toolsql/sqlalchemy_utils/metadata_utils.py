from __future__ import annotations

import sqlalchemy  # type: ignore
import toolcache

from .. import spec
from . import table_utils
from . import engine_utils


def create_metadata_object_from_schema(
    db_schema: spec.DBSchema,
) -> spec.SAMetadata:
    """create sqlalchemy metadata object reflecting table specs"""
    metadata = sqlalchemy.MetaData()
    for table_name, table_schema in db_schema['tables'].items():
        table_utils.create_table_object_from_schema(
            table_name=table_name,
            table_schema=table_schema,
            metadata=metadata,
        )
    return metadata


def create_metadata_object_from_db(
    *,
    engine: spec.SAEngine | None = None,
    db_config: spec.DBConfig | None = None,
    conn: spec.SAConnection | None = None,
) -> spec.SAMetadata:
    """create sqlalchemy metadata object reflecting all tables of database"""
    if engine is None:
        if db_config is not None:
            engine = engine_utils.create_engine(db_config=db_config)
        elif conn is not None:
            engine = conn.engine
        else:
            raise Exception('must specify engine, conn, or db_config')
    return _create_metadata_object_from_engine(engine=engine)


@toolcache.cache(cachetype='memory')
def _create_metadata_object_from_engine(
    engine: spec.SAEngine,
) -> spec.SAMetadata:
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine)
    return metadata
