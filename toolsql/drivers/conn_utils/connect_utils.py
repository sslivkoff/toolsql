from __future__ import annotations

from toolsql import drivers
from toolsql import spec
from . import uri_utils


def connect(
    target: str | spec.DBConfig,
    *,
    as_context: bool = True,
    autocommit: bool = True,
) -> spec.Connection:

    # determine uri
    if isinstance(target, str):
        uri = target
    elif isinstance(target, dict):
        uri = uri_utils.get_db_uri(db_config=target)
    else:
        raise Exception('must specify uri or db_config')

    # determine driver
    if isinstance(target, dict):
        driver = drivers.get_driver_class(db_config=target, sync=True)
    else:
        driver = drivers.get_driver_class(uri=uri, sync=True)

    # create connection
    return driver.connect(uri=uri, autocommit=autocommit, as_context=as_context)


def async_connect(
    target: str | spec.DBConfig,
    *,
    as_context: bool = True,
    autocommit: bool = True,
    timeout: int | None = None,
) -> spec.AsyncConnection:

    # determine uri
    if isinstance(target, str):
        uri = target
    elif isinstance(target, dict):
        uri = uri_utils.get_db_uri(db_config=target)
    else:
        raise Exception('must specify uri or db_config')

    # determine driver
    if isinstance(target, dict):
        driver = drivers.get_driver_class(db_config=target, sync=False)
    else:
        driver = drivers.get_driver_class(uri=uri, sync=False)

    # create connection
    return driver.async_connect(
        uri=uri, as_context=as_context, autocommit=autocommit
    )

