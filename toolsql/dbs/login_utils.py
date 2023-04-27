from __future__ import annotations

import os

import toolsql


def login(db_config: toolsql.DBConfig) -> None:
    """connect to db using interactive shell"""

    import subprocess

    env = os.environ.copy()
    if db_config['dbms'] == 'sqlite':
        path = db_config.get('path')
        if path is None:
            raise Exception('no path specified for sqlite database')
        cmd = ['sqlite3', path]

    elif db_config['dbms'] == 'postgresql':
        template = (
            'psql'
            ' --host={hostname}'
            ' --port={port}'
            ' --username={username}'
            ' {database}'
        )
        cmd = template.format(**db_config).split(' ')
        password = db_config.get('password')
        if password is not None:
            env['PGPASSWORD'] = password

    else:
        raise Exception('invalid dbms: ' + str(db_config['dbms']))

    subprocess.call(cmd, env=env)

