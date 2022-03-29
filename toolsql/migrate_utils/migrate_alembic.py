import os


def get_alembic_config_path(migrate_config):
    config_path = os.path.join(migrate_config['migrate_root'], 'alembic.ini')
    return config_path


def get_alembic_config(migrate_config):
    import alembic.config

    config_path = get_alembic_config_path(migrate_config)
    config = alembic.config.Config(config_path)
    return config


