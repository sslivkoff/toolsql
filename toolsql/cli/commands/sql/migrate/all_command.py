import toolsql


def get_command_spec():
    return {
        'f': migrate_all_command,
        'help': None,
        'inject': ['migrate_config'],
    }


def migrate_all_command(migrate_config):
    toolsql.create_and_apply_migration(migrate_config=migrate_config)

