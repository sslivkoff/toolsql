from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_purge_command,
        'help': None,
        'options': [
            {'name': 'target'},
            {'name': '--confirm', 'kwargs': {'action': 'store_true'}},
        ],
        'inject': ['migrate_config'],
    }


def migrate_purge_command(
    target: str,
    confirm: bool,
    migrate_config: toolcli.MigrateConfig,
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

