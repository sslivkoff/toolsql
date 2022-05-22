from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_status_command,
        'help': 'print migration status',
        'extra_data': ['migrate_config'],
    }


def migrate_status_command(migrate_config: toolsql.MigrateConfig) -> None:
    toolsql.print_migration_status(migrate_config=migrate_config)
