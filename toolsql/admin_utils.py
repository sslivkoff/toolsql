from . import sqlalchemy_utils


def create_tables(
    spec_metadata=None, db_schema=None, engine=None, db_config=None
):
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


def drop_tables(metadata, force=False):
    if not force:
        raise Exception('use force=True')
    print('dropping tables...')
    metadata.drop_all()
    print('...all tables dropped')


def drop_all_rows(metadata, conn=None, force=False, engine=None):
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

