from __future__ import annotations

from toolsql import spec
from . import driver_utils


def get_db_uri(db_config: spec.DBConfig) -> str:

    dbms = db_config['dbms']
    if dbms == 'postgresql':
        uri_template = (
            '{dbms}://{username}:{password}@{hostname}:{port}/{database}'
        )
        db_config = db_config.copy()
        db_config.setdefault('password', '')
        db_config.setdefault('hostname', 'localhost')
        db_config.setdefault('port', 5432)
    elif dbms == 'sqlite':
        uri_template = 'sqlite://{path}'
    else:
        raise Exception('unknown dbms: ' + str(dbms))

    return uri_template.format(**db_config)


#
# # connections
#


def connect(target: str | spec.DBConfig, *, as_context: bool = True) -> spec.Connection:

    if isinstance(target, str):
        uri = target
    elif isinstance(target, dict):
        uri = get_db_uri(db_config=target)
    else:
        raise Exception('must specify uri or db_config')
    driver = driver_utils.get_driver(uri=uri, sync=True)
    return driver.connect(uri=uri)


def async_connect(target: str | spec.DBConfig, *, as_context: bool = True) -> spec.AsyncConnection:

    if isinstance(target, str):
        uri = target
    elif isinstance(target, dict):
        uri = get_db_uri(db_config=target)
    else:
        raise Exception('must specify uri or db_config')
    driver = driver_utils.get_driver(uri=uri, sync=False)
    return driver.async_connect(uri=uri, as_context=as_context)


#
# # transactions
#

# def begin(
#     uri: str | None = None,
#     db_config: spec.DBConfig | None = None,
# ) -> spec.Transaction:

#     if uri is None:
#         if db_config is None:
#             raise Exception('must specify uri or db_config')
#         uri = get_db_uri(db_config=db_config)
#     driver = driver_utils.get_driver(uri=uri, db_config=db_config, sync=True)
#     return driver.begin(uri=uri)


# def begin_async(
#     uri: str | None = None,
#     db_config: spec.DBConfig | None = None,
# ) -> spec.AsyncTransaction:

#     if uri is None:
#         if db_config is None:
#             raise Exception('must specify uri or db_config')
#         uri = get_db_uri(db_config=db_config)
#     driver = driver_utils.get_driver(uri=uri, db_config=db_config, sync=True)
#     return driver.begin_async(uri=uri)

