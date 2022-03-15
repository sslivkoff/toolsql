import toolsql


def get_command_spec():
    return {
        'f': migrate_upgrade_command,
        'help': None,
        'options': [
            {'name': '--message'},
            {'name': '--noedit', 'kwargs': {'action': 'store_true'}},
            {'name': '--noautogenerate', 'kwargs': {'action': 'store_true'}},
        ],
        'inject': ['migrate_config'],
    }


def migrate_upgrade_command(migrate_config, message, noedit, noautogenerate, **kwargs):
    autogenerate = not noautogenerate
    toolsql.create_migration(
        migrate_config=migrate_config,
        message=message,
        autogenerate=autogenerate,
    )
    edit = not noedit
    if edit:
        toolsql.edit_migrations(migrate_config=migrate_config)

