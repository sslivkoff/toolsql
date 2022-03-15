import toolsql


def get_command_spec():
    return {
        'f': migrate_edit_command,
        'help': None,
        'options': [
            {'name': '--revision'},
        ],
        'inject': ['migrate_config'],
    }


def migrate_edit_command(migrate_config, revision, **kwargs):
    toolsql.edit_migrations(migrate_config=migrate_config, revision=revision)

