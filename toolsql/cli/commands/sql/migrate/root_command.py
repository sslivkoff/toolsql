def get_command_spec():
    return {
        'f': migrate_path_command,
        'help': None,
        'inject': ['migrate_config'],
    }


def migrate_path_command(migrate_config, **kwargs):
    print('migrate_root:', migrate_config['migrate_root'])

