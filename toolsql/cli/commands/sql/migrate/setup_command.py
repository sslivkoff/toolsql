from __future__ import annotations

import toolcli
import toolsql


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': migrate_setup_command,
        'help': 'setup migration root dir',
        'args': [{'name': '--confirm', 'action': 'store_true'}],
        'extra_data': ['migrate_config', 'db_schema', 'db_config'],
    }


def migrate_setup_command(
    migrate_config: toolsql.MigrateConfig, inject_calls, confirm: bool
) -> None:
    if 'db_schema' not in inject_calls:
        raise Exception('db_schema must be defined as a cli call injection')
    if 'db_config' not in inject_calls:
        raise Exception('db_config must be defined as a cli call injection')
    toolsql.setup_migrate_root(
        migrate_config=migrate_config,
        db_schema_call=inject_calls['db_schema'],
        db_config_call=inject_calls['db_config'],
        confirm=confirm,
    )
