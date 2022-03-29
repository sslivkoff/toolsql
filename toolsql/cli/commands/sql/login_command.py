from __future__ import annotations

import subprocess

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return{
        'f': login_command,
        'help': 'log in to database',
        'special': {'inject': ['db_config']},
    }


def login_command(db_config: toolsql.DBConfig) -> None:

    if db_config['dbms'] == 'postgresql':
        cmd = 'psql --dbname {database} --user {username}'
    else:
        raise NotImplementedError()
    cmd = cmd.format(**db_config)
    subprocess.call(cmd.split(' '))

