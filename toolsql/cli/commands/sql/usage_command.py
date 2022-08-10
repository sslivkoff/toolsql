from __future__ import annotations

import toolcli
import toolsql
import toolstr


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': usage_command,
        'help': 'print db usage information',
        'args': [{'name': '--full', 'action': 'store_true'}],
        'extra_data': ['db_config', 'db_schema', 'styles'],
    }


def usage_command(
    db_config: toolsql.DBConfig,
    db_schema: toolsql.DBSchema,
    full: bool,
    styles: toolcli.StyleTheme | None = None,
) -> None:

    toolsql.print_db_usage(
        db_config=db_config,
        db_schema=db_schema,
        full=full,
        styles=styles,
    )
