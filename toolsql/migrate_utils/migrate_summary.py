from .. import migrate_utils
from .. import sqlalchemy_utils


def print_migration_status(migrate_config):

    print('migration status')
    if not migrate_utils.migrate_root_is_setup(migrate_config):
        print('[migration dir does not exist]')
        print('[setup using `ROOT_CMD migrate setup`]')

    else:
        conn = sqlalchemy_utils.create_connection(
            db_config=migrate_config['db_config'],
        )
        current_revision = migrate_utils.get_current_revision(conn=conn)
        pending_revisions = migrate_utils.get_pending_revisions(migrate_config=migrate_config)

        print('- migrate_root:', migrate_config['migrate_root'])
        print('- current revision:', current_revision)
        if len(pending_revisions) == 0:
            print('- no pending revisions')
        else:
            if len(pending_revisions) == 1:
                print('- 1 pending revision:')
            else:
                print('-', len(pending_revisions), 'pending revisions:')
            for revision in pending_revisions:
                print('    -', revision.revision, revision.doc)

