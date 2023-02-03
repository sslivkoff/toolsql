from __future__ import annotations

from toolsql import spec


def create_db_config(
    db_config: spec.DBConfig,
    *,
    sync: bool | None,
) -> spec.DBConfig:
    """create new db_config"""

    db_config = db_config.copy()

    if sync is not None:

        # convert driver to corresponding version
        if sync:
            if db_config['driver'] in ['connectorx', 'sqlite3', 'psycopg']:
                pass
            elif db_config['driver'] == 'aiosqlite':
                db_config['driver'] = 'sqlite3'
            else:
                raise Exception('unknown driver: ' + str(db_config['driver']))
        else:
            if db_config['driver'] in ['connectorx', 'aiosqlite', 'psycopg']:
                pass
            elif db_config['driver'] == 'sqlite3':
                db_config['driver'] = 'aiosqlite'
            else:
                raise Exception('unknown driver: ' + str(db_config['driver']))

    return db_config

