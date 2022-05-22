from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_all_command,
        'help': 'migrate all',
        'extra_data': ['migrate_config'],
    }


def migrate_all_command(migrate_config: toolsql.MigrateConfig) -> None:
    toolsql.create_and_apply_migration(migrate_config=migrate_config)
