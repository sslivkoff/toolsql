from __future__ import annotations

import functools

from toolsql import spec


def get_db_uri(db_config: spec.DBConfig) -> str:

    dbms = db_config['dbms']
    if dbms == 'postgresql':
        if db_config.get('password') in [None, '']:
            uri_template = (
                '{dbms}://{username}@{hostname}:{port}/{database}'
            )
        else:
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
            username, password = username.split(':')
        else:
            password = None

        hostname, _ = tail.split('/')
        if ':' in hostname:
            hostname, port_str = hostname.split(':')
            port: int | None = int(port_str)
        else:
            port = None

        return {
            'dbms': 'postgresql',
            'database': database,
            'username': username,
            'hostname': hostname,
            'port': port,
            'password': password,
        }
    else:
        raise Exception('unknown uri format')

