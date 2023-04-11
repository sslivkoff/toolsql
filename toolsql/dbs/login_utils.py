from __future__ import annotations

import toolsql


def login(db_config: toolsql.DBConfig) -> None:
    """connect to db using interactive shell"""

    import subprocess

    if db_config['dbms'] == 'sqlite':
        path = db_config.get('path')
        if path is None:
            raise Exception('no path specified for sqlite database')
        cmd = ['sqlite3', path]

    elif db_config['dbms'] == 'postgresql':
        raise NotImplementedError()

    else:
        raise Exception('invalid dbms: ' + str(db_config['dbms']))

    subprocess.call(cmd)

