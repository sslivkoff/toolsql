from __future__ import annotations

import functools

from toolsql import spec


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


@functools.lru_cache()
def parse_uri(uri: str) -> spec.DBConfig:
    if uri.startswith('sqlite://'):
        path = uri.split('sqlite://')[1]
        return {
            'dbms': 'sqlite',
            'path': path,
        }
    elif uri.startswith('postgres'):
        database = uri[uri.rfind('/') + 1 :]
        head, tail = uri.split('@')
        username = head.split('/')[-1]
        if ':' in username:
            username = username.split(':')[0]

        return {
            'dbms': 'postgresql',
            'database': database,
            'username': username,
        }
    else:
        raise Exception('unknown uri format')
