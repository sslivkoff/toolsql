from __future__ import annotations

from toolsql import spec
from .dbms_classes import abstract_dbms


def get_dbms_class(
    *,
    name: str | None = None,
    uri: str | None = None,
    db_config: spec.DBConfig | None = None,
) -> type[abstract_dbms.AbstractDbms]:

    dbms = _get_dbms_name(
        name=name,
        uri=uri,
        db_config=db_config,
    )

    if dbms == 'sqlite':
        from .dbms_classes import sqlite_dbms

        return sqlite_dbms.SqliteDbms
    elif dbms == 'postgresql':
        from .dbms_classes import postgresql_dbms

        return postgresql_dbms.PostgresqlDbms
    else:
        raise Exception('unknown dbms: ' + str(dbms))


def _get_dbms_name(
    *,
    name: str | None = None,
    uri: str | None = None,
    db_config: spec.DBConfig | None = None,
) -> str:

    if name is not None:
        return name
    elif uri is not None:
        head, tail = uri.split('://')
        if '+' in head:
            dbms = head.split('+')[0]
        else:
            dbms = head
        if dbms in ['postgresql', 'sqlite']:
            return dbms
        else:
            raise Exception('unknown dbms: ' + str(dbms))
    elif db_config is not None:
        return db_config['dbms']
    else:
        raise Exception('must specify uri, db_config, or conn')

