from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_path_command,
        'help': 'print migration root dir',
        'special': {
            'inject': ['migrate_config'],
        },
    }


def migrate_path_command(migrate_config: toolsql.MigrateConfig) -> None:
    print('migrate_root:', migrate_config['migrate_root'])

