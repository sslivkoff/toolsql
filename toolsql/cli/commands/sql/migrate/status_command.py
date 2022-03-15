import toolsql


def get_command_spec():
    return {
        'f': migrate_status_command,
        'help': None,
        'inject': ['migrate_config'],
    }


def migrate_status_command(migrate_config, **kwargs):
    toolsql.print_migration_status(migrate_config=migrate_config)

