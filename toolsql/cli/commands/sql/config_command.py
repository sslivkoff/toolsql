from __future__ import annotations

import json

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': config_command,
        'help': 'print config',
        'extra_data': ['db_config'],
    }


def config_command(db_config: toolsql.DBConfig) -> None:
    as_str = json.dumps(db_config, indent=4, sort_keys=True)
    print(as_str)
