from __future__ import annotations

import os

from toolsql import drivers
from toolsql import spec


def does_db_exist(db: str | spec.DBConfig) -> bool:

    if isinstance(db, str):
        db_config = drivers.parse_uri(db)
    elif isinstance(db, dict):
        db_config = db
    else:
        raise Exception('unknown db specification')

    if db_config['driver'] == 'connectorx':
        if db_config['dbms'] == 'sqlite':
            db_config = dict(db_config, driver='sqlite3')  # type: ignore
        elif db_config['dbms'] == 'postgresql':
            db_config = dict(db_config, driver='psycopg')  # type: ignore
        else:
            raise Exception()

    if db_config['dbms'] == 'sqlite':
        path = db_config['path']
        if path is None:
            raise Exception('sqlite path not specified')
        return os.path.isfile(path)
    elif db_config['dbms'] == 'postgresql':
        try:
            with drivers.connect(db_config):
                pass
            return True
        except Exception:
            return False
    else:
        raise Exception('unknown dbms: ' + str(db_config['dbms']))

