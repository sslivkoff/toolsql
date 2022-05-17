from __future__ import annotations

import toolsql
import toolcli


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': schema_command,
        'help': 'print schema of database',
        'args': [
            {'name': '--full', 'action': 'store_true'},
            {'name': '--json', 'action': 'store_true'},
        ],
        'extra_data': ['db_config', 'db_schema'],
    }


def schema_command(
    db_config: toolsql.DBConfig,
    db_schema: toolsql.DBSchema,
    full: bool,
    json: bool,
):
    if json:
        import json as json_module

        print(json_module.dumps(db_schema, indent=4))
    else:
        toolsql.print_schema(
            db_config=db_config, db_schema=db_schema, full=full
        )
