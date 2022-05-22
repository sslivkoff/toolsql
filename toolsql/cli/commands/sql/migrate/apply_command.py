from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_apply_command,
        'help': 'apply migrations',
        'extra_data': ['migrate_config'],
    }


def migrate_apply_command(migrate_config: toolsql.MigrateConfig) -> None:
    toolsql.apply_migrations(migrate_config=migrate_config)
