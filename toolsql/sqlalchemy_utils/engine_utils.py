"""functions for managing sqlalchemy engines and connections"""

import logging

import sqlalchemy
import sqlalchemy.dialects.sqlite
import sqlite3

from .. import exceptions


def create_engine(
    *,
    dbms=None,
    database=None,
    username=None,
    password=None,
    path=None,
    log_level=None,
    db_config=None,
    engine_kwargs=None,
):
    """create sqlalchemy engine object"""

    # set logger
    if log_level is not None:
        logging.getLogger('sqlalchemy.engine').setLevel(log_level)

    # create engine uri
    uri = get_db_uri(
        dbms=dbms,
        database=database,
        username=username,
        password=password,
        path=path,
        db_config=db_config,
    )

    # create engine
    engine_kwargs = _process_engine_kwargs(
        engine_kwargs=engine_kwargs,
        uri=uri,
        db_config=db_config,
    )
    engine = sqlalchemy.create_engine(uri, **engine_kwargs)
    _add_exception_handler(engine)

    return engine


def get_db_uri(
    *,
    dbms=None,
    server=None,
    database=None,
    username=None,
    password=None,
    path=None,
    db_config=None,
):
    """create database uri for use with a sqlalchemy engine object"""

    # ensure dbms is specified
    if db_config is not None and db_config.get('dbms') is not None:
        dbms = db_config['dbms']
    if dbms is None:
        raise Exception('must specify dbms')

    # specify template
    if db_config is not None:
        db_kwargs = dict(db_config)
    else:
        db_kwargs = {}
    if dbms == 'postgres':
        if server is not None:
            db_kwargs['server'] = server
        if database is not None:
            db_kwargs['database'] = database
        if username is not None:
            db_kwargs['username'] = username
        if password is not None:
            db_kwargs['password'] = password
        if db_kwargs.get('port') is None:
            db_kwargs['port'] = 5432
        uri_template = 'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}'
    elif dbms == 'sqlite':
        if path is not None:
            db_kwargs['path'] = path
        uri_template = 'sqlite:///{path}'
    elif dbms == 'sqlite_memory':
        uri_template = 'sqlite:///:memory'
    else:
        raise Exception('unrecognized db: ' + str(dbms))

    # create uri
    db_kwargs = {k: v for k, v in db_kwargs.items() if v is not None}
    uri = uri_template.format(**db_kwargs)

    return uri


def _process_engine_kwargs(engine_kwargs, uri, db_config):
    if engine_kwargs is None:
        engine_kwargs = {}
    else:
        engine_kwargs = dict(engine_kwargs)

    # TODO: allow db_config to change these defaults
    # engine_kwargs.setdefault('poolclass', sqlalchemy.pool.NullPool)
    # if uri.startswith('sqlite:///'):
    #     engine_kwargs.setdefault('check_same_thread', False)

    return engine_kwargs


def _add_exception_handler(engine):
    """add custom event handler to sqlalchemy engine"""

    @sqlalchemy.event.listens_for(engine, 'handle_error', retval=True)
    def handle_error(context):
        exception = context.original_exception
        if isinstance(
            exception, (sqlite3.InterfaceError, sqlite3.IntegrityError,),
        ):
            return exceptions.InvalidOperationException(exception.args)

