from __future__ import annotations

import subprocess

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return{
        'f': login_command,
        'help': 'log in to database',
        'extra_data': ['db_config'],
    }


def login_command(db_config: toolsql.DBConfig) -> None:

    if db_config['dbms'] == 'postgresql':
        print('connecting to postgres database:', db_config['database'])
        print()
        cmd = 'psql --dbname {database} --user {username}'
    elif db_config['dbms'] == 'sqlite':
        print('connecting to sqlite3 database:', db_config['path'])
        print()
        cmd = 'sqlite3 {path}'
    else:
        raise NotImplementedError()
    cmd = cmd.format(**db_config)
    subprocess.call(cmd.split(' '))
