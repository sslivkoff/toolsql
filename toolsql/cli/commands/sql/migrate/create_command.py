from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_upgrade_command,
        'help': None,
        'options': [
            {'name': '--message'},
            {'name': '--noedit', 'kwargs': {'action': 'store_true'}},
            {'name': '--noautogenerate', 'kwargs': {'action': 'store_true'}},
        ],
        'special': {
            'inject': ['migrate_config'],
        },
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

