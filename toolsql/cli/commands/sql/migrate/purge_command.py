from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_purge_command,
        'help': 'purge migrations',
        'args': [
            {'name': 'target'},
            {'name': '--confirm', 'action': 'store_true'},
        ],
        'extra_data': ['migrate_config'],
    }


def migrate_purge_command(
    target: str,
    confirm: bool,
    migrate_config: toolsql.MigrateConfig,
) -> None:
    if target == 'pending':
        toolsql.purge_pending_migrations(
            migrate_config=migrate_config,
            confirm=confirm,
        )
    elif target == 'all':
        toolsql.purge_all_migrations(
            migrate_config=migrate_config,
            confirm=confirm,
        )
    else:
        raise Exception('purge target should be "pending" or "all"')
