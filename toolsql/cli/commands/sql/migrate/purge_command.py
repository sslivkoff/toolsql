import toolsql


def get_command_spec():
    return {
        'f': migrate_purge_command,
        'help': None,
        'options': [
            {'name': 'target'},
            {'name': '--confirm', 'kwargs': {'action': 'store_true'}},
        ],
        'inject': ['migrate_config'],
    }


def migrate_purge_command(target, confirm, migrate_config, **kwargs):
    if target == 'pending':
        toolsql.purge_pending_migrations(
            migrate_config=migrate_config, confirm=confirm,
        )
    elif target == 'all':
        toolsql.purge_all_migrations(
            migrate_config=migrate_config, confirm=confirm,
        )
    else:
        raise Exception('purge target should be "pending" or "all"')

