from __future__ import annotations

import subprocess

import sqlalchemy  # type: ignore

from . import sqlalchemy_utils
from . import spec


def print_schema(
    *,
    db_config: spec.DBConfig | None = None,
    db_metadata: spec.SAMetadata | None = None,
    spec_metadata: spec.SAMetadata | None = None,
    engine: spec.SAEngine | None = None,
    db_schema: spec.DBSchema | None = None,
    full: bool = False,
) -> None:

    # obtain metadata
    if db_metadata is None:
        if engine is None and db_config is None:
            raise Exception('must specify db_metadata, engine, or db_config')
        db_metadata = sqlalchemy_utils.create_metadata_object_from_db(
            db_config=db_config,
            engine=engine,
        )
    if spec_metadata is None:
        if db_schema is None:
            raise Exception('must specify spec_metadata or db_schema')
        spec_metadata = sqlalchemy_utils.create_metadata_object_from_schema(
            db_schema=db_schema,
        )

    # print tables
    table_names = list(db_metadata.tables.keys())
    n_db_tables = len(db_metadata.tables.keys())
    n_spec_tables = len(spec_metadata.tables.keys())
    print('schema (' + str(n_db_tables) + '/' + str(n_spec_tables) + ' tables)')
    for table_name in sorted(table_names):
        n_db_columns = len(db_metadata.tables[table_name].columns)
        n_spec_columns = len(spec_metadata.tables[table_name].columns)
        print(
            '- ' + table_name,
            '(' + str(n_db_columns) + '/' + str(n_spec_columns) + ' columns)',
        )
    for name, table in spec_metadata.tables.items():
        if name not in db_metadata.tables:
            print('- missing', name, 'table')

    if full:
        metadatas = {'spec': spec_metadata, 'db': db_metadata}
        for name, metadata in metadatas.items():
            print(name, 'tables:')
            for table_name, table in spec_metadata.tables.items():
                print('-', table_name)
                for column_name, column in table.columns.items():
                    print('    -', column_name, repr(column))
            print()
            print()


def print_usage(
    *,
    conn: spec.SAConnection,
    db_config: spec.DBConfig | None = None,
    db_metadata: spec.SAMetadata | None = None,
    spec_metadata: spec.SAMetadata | None = None,
    engine: spec.SAEngine | None = None,
    db_schema: spec.DBSchema | None = None,
) -> None:
    if db_metadata is None:
        if engine is None and db_config is None:
            raise Exception('must specify db_metadata, engine, or db_config')
        db_metadata = sqlalchemy_utils.create_metadata_object_from_db(
            db_config=db_config,
            engine=engine,
        )
    if spec_metadata is None:
        if db_schema is None:
            raise Exception('must specify spec_metadata or db_schema')
        spec_metadata = sqlalchemy_utils.create_metadata_object_from_schema(
            db_schema=db_schema,
        )

    print('usage')
    if len(db_metadata.tables.keys()) == 0:
        print('[no tables exist]')
    for name, table in db_metadata.tables.items():

        # get row count
        statement = sqlalchemy.select([sqlalchemy.func.count()]).select_from(  # type: ignore
            table
        )

        row_count = conn.execute(statement).scalar()

        print('-', table, '(' + str(row_count) + ' rows)')

    for name, table in spec_metadata.tables.items():
        if name not in db_metadata.tables:
            print('- missing', name, 'table')


def get_bytes_usage_per_table(
    db_config: spec.DBConfig,
    include_indices: bool = True,
) -> dict[str, int]:
    if db_config['dbms'] != 'postgresql':
        raise NotImplementedError()

    if include_indices:
        cmd = """psql --dbname {database} --user {username} -c "\dti+" | awk -F "|" '{print $2} {print $6}'"""
    else:
        cmd = """psql --dbname {database} --user {username} -c "\dt+" | awk -F "|" '{print $2} {print $5}'"""

    for key, value in db_config.items():
        format_key = '{' + key + '}'
        if format_key in cmd:
            cmd = cmd.replace(format_key, str(value))

    output = subprocess.check_output(cmd, shell=True, universal_newlines=True)
    output = output.strip()
    lines = output.split('\n')
    lines = lines[2:]
    lines = [line.strip() for line in lines]
    lines = [line for line in lines if line != '']
    if len(lines) % 2 != 0:
        raise Exception('could not parse lines')

    bytes_usage = {}
    for l in range(0, len(lines), 2):
        entity = lines[l]
        size = lines[l + 1]
        bytes_usage[entity] = int(size)

    return bytes_usage


def get_bytes_usage_for_database(db_config: spec.DBConfig) -> int:
    cmd = """psql --dbname {database} --user {username} -c "\l+" | grep {database} | awk -F "|" '{print $1} {print $7}'"""
    output = subprocess.check_output(cmd, shell=True, universal_newlines=True)
    output = output.strip()
    lines = output.split('\n')
    lines = [line.strip() for line in lines]
    if len(lines) != 2:
        raise Exception('could not parse output')
    database = lines[0]
    bytes_usage = lines[1]
    if database != db_config['database']:
        raise Exception('could not parse output')
    return int(bytes_usage)

