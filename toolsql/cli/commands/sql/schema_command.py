import toolsql


def get_command_spec():
    return {
        'f': schema_command,
        'help': None,
        'options': [
            {'name': '--full', 'kwargs': {'action': 'store_true'}},
            {'name': '--json', 'kwargs': {'action': 'store_true'}},
        ],
        'inject': ['db_config', 'db_schema'],
    }


def schema_command(db_config, db_schema, full, json, **kwargs):
    if json:
        import json as json_module

        print(json_module.dumps(db_schema, indent=4))
    else:
        toolsql.print_schema(db_config=db_config, db_schema=db_schema, full=full)

