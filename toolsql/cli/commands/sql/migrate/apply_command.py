import toolsql


def get_command_spec():
    return {
        'f': migrate_apply_command,
        'help': None,
        'inject': ['migrate_config'],
    }


def migrate_apply_command(migrate_config, **kwargs):
    toolsql.apply_migrations(migrate_config=migrate_config)

