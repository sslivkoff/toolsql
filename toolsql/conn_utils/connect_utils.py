from __future__ import annotations

from toolsql import driver_utils
from toolsql import spec
from . import uri_utils


def connect(
    target: str | spec.DBConfig,
    *,
    as_context: bool = True,
    autocommit: bool = True,
) -> spec.Connection:

    if isinstance(target, str):
        uri = target
    elif isinstance(target, dict):
        uri = uri_utils.get_db_uri(db_config=target)
    else:
        raise Exception('must specify uri or db_config')
    if isinstance(target, dict):
        driver = driver_utils.get_driver_class(
            driver=target['driver'], sync=True
        )
    else:
        driver = driver_utils.get_driver_class(uri=uri, sync=True)
    return driver.connect(uri=uri, autocommit=autocommit, as_context=as_context)


def async_connect(
    target: str | spec.DBConfig,
    *,
    as_context: bool = True,
    autocommit: bool = True,
) -> spec.AsyncConnection:

    if isinstance(target, str):
        uri = target
    elif isinstance(target, dict):
        uri = uri_utils.get_db_uri(db_config=target)
    else:
        raise Exception('must specify uri or db_config')
    if isinstance(target, dict):
        driver = driver_utils.get_driver_class(
            driver=target['driver'], sync=True
        )
    else:
        driver = driver_utils.get_driver_class(uri=uri, sync=True)
    return driver.async_connect(
        uri=uri, as_context=as_context, autocommit=autocommit
    )

