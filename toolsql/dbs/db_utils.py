from __future__ import annotations

from toolsql import spec
from .db_classes import abstract_db


def get_db_class(
    *,
    name: str | None = None,
    uri: str | None = None,
    db_config: spec.DBConfig | None = None,
) -> type[abstract_db.AbstractDb]:

    db = _get_db_name(
        name=name,
        uri=uri,
        db_config=db_config,
    )

    if db == 'sqlite':
        from .db_classes import sqlite_db

        return sqlite_db.SqliteDb
    elif db == 'postgresql':
        from .db_classes import postgresql_db

        return postgresql_db.PostgresqlDb
    else:
        raise Exception('unknown db: ' + str(db))


def _get_db_name(
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
            db = head.split('+')[0]
        else:
            db = head
        if db in ['postgresql', 'sqlite']:
            return db
        else:
            raise Exception('unknown db: ' + str(db))
    elif db_config is not None:
        return db_config['dbms']
    else:
        raise Exception('must specify uri, db_config, or conn')

