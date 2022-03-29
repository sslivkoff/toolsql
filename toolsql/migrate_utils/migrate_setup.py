import os
import sys
import textwrap

import toolcli

from . import migrate_alembic


def migrate_root_is_setup(migrate_config):
    config_path = migrate_alembic.get_alembic_config_path(migrate_config)
    is_setup = os.path.isfile(config_path)
    return is_setup


def setup_migrate_root(migrate_config, db_schema_call, db_config_call, confirm=False):
    import alembic.command

    migrate_root = migrate_config['migrate_root']

    # validate db calls
    calls = {'db_schema_call': db_schema_call, 'db_config_call': db_config_call}
    for name, call in calls.items():
        if set(call.keys()) != {'module', 'function'}:
            raise Exception(name + ' should have keys "module" and "function"')
    for name, call in calls.items():
        for key, value in call.items():
            if not isinstance(value, str):
                raise Exception(str(key) + ' in ' + name + ' should be str')

    # check if exists
    if migrate_root_is_setup(migrate_config):
        print('migrate root already setup in', migrate_root)
        return

    # confirm setup
    if not confirm:
        print('setting up migrate root')
        print()
        print('will create migrate root: ' + migrate_root)
        response = toolcli.input_yes_or_no('continue? ')
        if not response:
            sys.exit()

    # change to parent of service migration path
    if not os.path.isdir(migrate_root):
        os.makedirs(migrate_root)
    os.chdir(os.path.join(migrate_root, '..'))

    # create migrate_root
    alembic_config = migrate_alembic.get_alembic_config(migrate_config)
    alembic.command.init(config=alembic_config, directory=migrate_root)

    # customize env.py to load service metadata
    _setup_env_py(migrate_config=migrate_config, **calls)

    print()
    print('alembic now configured')
    print('migration dir now setup')


def _setup_env_py(migrate_config, db_config_call, db_schema_call):

    # define snippets
    snippets = [
        {
            # metadata loading
            'before': 'target_metadata = None',
            'after': """
                import toolsql
                import {db_schema_module}
                db_schema = {db_schema_module}.{db_schema_function}()
                target_metadata = toolsql.create_metadata_object_from_schema(db_schema=db_schema)
            """,
        },
        {
            # uri loading
            'before': 'fileConfig(config.config_file_name)',
            'after': """
                import toolsql
                import {db_config_module}
                fileConfig(config.config_file_name)
                db_config = {db_config_module}.{db_config_function}()
                db_uri = toolsql.get_db_uri(db_config=db_config)
                config.set_main_option("sqlalchemy.url", db_uri)
            """,
        },
    ]
    for s in range(len(snippets)):
        snippets[s]['before'] = textwrap.dedent(snippets[s]['before'])
        snippets[s]['after'] = textwrap.dedent(snippets[s]['after'])
        snippets[s]['after'] = snippets[s]['after'].format(
            db_config_module=db_config_call['module'],
            db_config_function=db_config_call['function'],
            db_schema_module=db_schema_call['module'],
            db_schema_function=db_schema_call['function'],
        )

    # replace env.py content
    env_path = os.path.join(migrate_config['migrate_root'], 'env.py')
    with open(env_path, 'r') as f:
        old_content = f.read()
    new_content = old_content
    for snippet in snippets:
        new_content = new_content.replace(
            snippet['before'], snippet['after'], 1
        )
    with open(env_path, 'w') as f:
        f.write(new_content)

