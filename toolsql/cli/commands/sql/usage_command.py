from __future__ import annotations

import toolsql
import toolcli


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': usage_command,
        'help': None,
        'args': [{'name': '--full', 'action': 'store_true'}],
        'special': {
            'inject': ['db_config', 'db_schema'],
        },
    }


def usage_command(
    db_config: toolsql.DBConfig, db_schema: toolsql.DBSchema, full: bool
) -> None:

    toolsql.print_usage(db_config=db_config, db_schema=db_schema)
    if full:
        bytes_usage_per_table = toolsql.get_bytes_usage_per_table(
            db_config=db_config
        )
        print(bytes_usage_per_table)

    bytes_usage_for_database = toolsql.get_bytes_usage_for_database(
        db_config=db_config
    )
    print()
    print('total database usage:')
    print(bytes_usage_for_database)

