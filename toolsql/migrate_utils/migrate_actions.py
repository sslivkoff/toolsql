import os
import shutil
import sys
import time

import toolcli

from .. import sqlalchemy_utils
from .. import migrate_utils
from . import migrate_alembic
from . import migrate_setup
from . import migrate_revisions


def create_migration(migrate_config, message, autogenerate=True):
    """create migration"""
    import alembic.command
    import alembic.util

    if message is None:
        message = time.strftime('%Y-%m-%d__%H-%M-%S', time.localtime())
    print('creating migration', message)

    if not migrate_setup.migrate_root_is_setup(migrate_config):
        raise Exception('migration dir not setup')

    os.chdir(migrate_config['migrate_root'])
    alembic_config = migrate_alembic.get_alembic_config(migrate_config)

    try:
        scripts = alembic.command.revision(
            config=alembic_config, message=message, autogenerate=autogenerate,
        )
    except alembic.util.CommandError as e:
        if e.args[0] == 'Target database is not up to date.':
            print('[error] database not sync\'d with existing revisions')
            print('delete unused revisions or run `alembic stamp head`')
            sys.exit()
        else:
            raise e

    return scripts


def edit_migrations(migrate_config, revision=None):
    """edit pending migrations"""
    if revision is not None:
        raise NotImplementedError()

    pending_revisions = migrate_revisions.get_pending_revisions(migrate_config)
    paths = [revision.path for revision in pending_revisions]
    if len(paths) == 0:
        print('no paths to edit')
        return
    else:
        print('editing:')
        for path in paths:
            print(path)
        toolcli.open_file_in_editor(paths)


def apply_migrations(migrate_config):
    """apply pending migrations"""
    import alembic.command

    os.chdir(migrate_config['migrate_root'])
    alembic_config = migrate_alembic.get_alembic_config(migrate_config)
    alembic.command.upgrade(alembic_config, 'head')


def create_and_apply_migration(migrate_config):
    """create, edit, and apply migrations"""
    create_migration(migrate_config=migrate_config, message=None)
    edit_migrations(migrate_config=migrate_config)
    apply_migrations(migrate_config=migrate_config)


def downgrade_migration(migrate_config):
    raise NotImplementedError()


def purge_pending_migrations(migrate_config, confirm=False):

    if not confirm:
        raise Exception('are you sure? use confirm=True')

    pending_revisions = migrate_utils.get_pending_revisions(
        migrate_config=migrate_config,
    )

    if len(pending_revisions) > 0:
        for revision in pending_revisions:
            print('deleting', revision.doc)
            os.remove(revision.path)
        print()
        print('deleted all', len(pending_revisions), 'pending revisions')
    else:
        print('no pending revisions to delete')


def purge_all_migrations(migrate_config, confirm=False):
    """delete migrate root and all migration history"""

    if not confirm:
        raise Exception('are you sure? use confirm=True')

    # obtain confirmation
    migrate_root = migrate_config['migrate_root']
    print('deleting migration data')
    print('- path =', migrate_root)
    print()
    response = toolcli.input_yes_or_no('continue? ')
    if not response:
        sys.exit()

    # delete migration dir
    if os.path.isdir(migrate_root):
        shutil.rmtree(migrate_root)

    # delete migration table
    engine = sqlalchemy_utils.create_engine(
        db_config=migrate_config['db_config']
    )
    conn = sqlalchemy_utils.create_connection(engine=engine)
    if engine.dialect.has_table(conn, 'alembic_version'):
        conn.execute('DROP TABLE alembic_version')

    print()
    print('migration dir and migration-tracking table are now gone')

