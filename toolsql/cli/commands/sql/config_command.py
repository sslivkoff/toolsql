import json


def get_command_spec():
    return {
        'f': config_command,
        'help': 'print config',
        'options': [],
        'inject': ['db_config'],
    }


def config_command(db_config, **kwargs):
    as_str = json.dumps(db_config, indent=True, sort_keys=True)
    print(as_str)

