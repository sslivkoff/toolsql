from __future__ import annotations

import toolcli
import toolsql
import toolstr


def get_command_spec() -> toolcli.CommandSpec:
    return {
        'f': usage_command,
        'help': 'print db usage information',
        'args': [{'name': '--full', 'action': 'store_true'}],
        'extra_data': ['db_config', 'db_schema'],
    }


def usage_command(
    db_config: toolsql.DBConfig, db_schema: toolsql.DBSchema, full: bool
) -> None:

    toolstr.print_text_box('Database Usage')

    # print bytes in database
    bytes_usage_for_database = toolsql.get_bytes_usage_for_database(
        db_config=db_config
    )
    print('- storage size:', toolstr.format_nbytes(bytes_usage_for_database))

    # print row counts
    engine = toolsql.create_engine(db_config=db_config)
    print()
    with engine.connect() as conn:
        toolsql.print_row_counts(conn=conn, db_config=db_config, db_schema=db_schema)

    # print byte usage per table
    if full:
        bytes_usage_per_table = toolsql.get_bytes_usage_per_table(
            db_config=db_config
        )
        print(bytes_usage_per_table)

