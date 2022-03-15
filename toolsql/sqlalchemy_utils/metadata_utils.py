import sqlalchemy
import toolcache

from . import table_utils
from . import engine_utils


def create_metadata_object_from_schema(db_schema):
    """create sqlalchemy metadata object reflecting table specs"""
    metadata = sqlalchemy.MetaData()
    for table_name, table_schema in db_schema['tables'].items():
        table_utils.create_table_object_from_schema(
            table_name=table_name, table_schema=table_schema, metadata=metadata,
        )
    return metadata


def create_metadata_object_from_db(*, engine=None, db_config=None, conn=None):
    """create sqlalchemy metadata object reflecting all tables of database"""
    if engine is None:
        if db_config is not None:
            engine = engine_utils.create_engine(db_config=db_config)
        elif conn is not None:
            engine = conn.engine
        else:
            raise Exception('must specify engine, conn, or db_config')
    return _create_metadata_object_from_engine(engine=engine)


@toolcache.cache(cache_type='memory')
def _create_metadata_object_from_engine(engine):
    return sqlalchemy.MetaData(bind=engine, reflect=True)

