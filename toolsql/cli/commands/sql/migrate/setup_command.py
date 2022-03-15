import toolsql


def get_command_spec():
    return {
        'f': migrate_setup_command,
        'help': None,
        'options': [{'name': '--confirm', 'kwargs': {'action': 'store_true'}}],
        'inject': ['migrate_config', 'inject_calls'],
    }


def migrate_setup_command(migrate_config, inject_calls, confirm, **kwargs):
    if 'db_schema' not in inject_calls:
        raise Exception('db_schema must be defined as a cli call injection')
    if 'db_config' not in inject_calls:
        raise Exception('db_config must be defined as a cli call injection')
    toolsql.setup_migrate_root(
        migrate_config=migrate_config,
        db_schema_call=inject_calls['db_schema'],
        db_config_call=inject_calls['db_config'],
        confirm=confirm,
    )

