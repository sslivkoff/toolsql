from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_edit_command,
        'help': 'edit migrations',
        'args': [
            {'name': '--revision'},
        ],
        'extra_data': ['migrate_config'],
    }


def migrate_edit_command(
    migrate_config: toolsql.MigrateConfig,
    revision: str,
) -> None:
    toolsql.edit_migrations(migrate_config=migrate_config, revision=revision)
