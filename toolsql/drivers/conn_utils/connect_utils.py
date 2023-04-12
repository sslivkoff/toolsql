from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from . import uri_utils


def connect(
    target: str | spec.DBConfig,
    *,
    as_context: bool = True,
    autocommit: bool = True,
    extra_kwargs: typing.Mapping[str, typing.Any] | None = None,
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
    try:
        return driver.connect(
            uri=uri,
            autocommit=autocommit,
            as_context=as_context,
            extra_kwargs=extra_kwargs,
        )
    except Exception as e:
        raise spec.CannotConnect(*e.args)


def async_connect(
    target: str | spec.DBConfig,
    *,
    as_context: bool = True,
    autocommit: bool = True,
    timeout: int | None = None,
    extra_kwargs: typing.Mapping[str, typing.Any] | None = None,
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
    try:
        return driver.async_connect(
            uri=uri,
            as_context=as_context,
            autocommit=autocommit,
            extra_kwargs=extra_kwargs,
        )
    except Exception as e:
        raise spec.CannotConnect(*e.args)

