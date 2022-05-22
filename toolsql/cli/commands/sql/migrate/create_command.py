from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_upgrade_command,
        'help': 'create migrations',
        'args': [
            {'name': '--message'},
            {'name': '--noedit', 'action': 'store_true'},
            {'name': '--noautogenerate', 'action': 'store_true'},
        ],
        'extra_data': ['migrate_config'],
    }


def migrate_upgrade_command(
    migrate_config: toolsql.MigrateConfig,
    message: str,
    noedit: bool,
    noautogenerate: bool,
) -> None:
    autogenerate = not noautogenerate
    toolsql.create_migration(
        migrate_config=migrate_config,
        message=message,
        autogenerate=autogenerate,
    )
    edit = not noedit
    if edit:
        toolsql.edit_migrations(migrate_config=migrate_config)
