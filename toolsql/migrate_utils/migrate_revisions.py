from .. import sqlalchemy_utils


def get_current_revision(*, conn=None, migrate_config=None):
    import alembic.migration

    if conn is None:
        if migrate_config is None:
            raise Exception('must specify conn or migrate_config')
        db_config = migrate_config['db_config']
        conn = sqlalchemy_utils.create_connection(db_config=db_config)
    context = alembic.migration.MigrationContext.configure(conn)
    current_revision = context.get_current_revision()
    return current_revision


def get_pending_revisions(migrate_config):
    import alembic.script

    current_revision_id = get_current_revision(migrate_config=migrate_config)
    script_dir = alembic.script.ScriptDirectory(migrate_config['migrate_root'])
    pending = []
    for revision in script_dir.walk_revisions():
        if revision.revision == current_revision_id:
            break
        else:
            pending.append(revision)
    return pending

