from __future__ import annotations

import typing
import os
import subprocess

import sqlalchemy  # type: ignore
import toolstr

from . import sqlalchemy_utils
from . import spec


def get_missing_tables(
    db_config: spec.DBConfig | None = None,
    db_metadata: spec.SAMetadata | None = None,
    spec_metadata: spec.SAMetadata | None = None,
    engine: spec.SAEngine | None = None,
    db_schema: spec.DBSchema | None = None,
) -> typing.Mapping[str, typing.Sequence[str]]:
    """return list of tables present in spec but missing in db"""

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

    missing_from_db = [
        name
        for name, table in spec_metadata.tables.items()
        if name not in db_metadata.tables
    ]
    missing_from_spec = [
        name
        for name, table in db_metadata.tables.items()
        if name not in spec_metadata.tables
    ]
    return {
        'missing_from_db': missing_from_db,
        'missing_from_spec': missing_from_spec,
    }


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
    toolstr.print_text_box('Schema Summary')
    print('- tables specified in schema:', n_spec_tables)
    print('- tables existing in database:', n_db_tables)
    print()
    toolstr.print_header('Database Tables (' + str(len(table_names)) + ')')
    for table_name in sorted(table_names):
        # n_db_columns = len(db_metadata.tables[table_name].columns)
        # if table_name in spec_metadata.tables:
        #     n_spec_columns: int | str = len(
        #         spec_metadata.tables[table_name].columns
        #     )
        # else:
        #     n_spec_columns = '?'
        print(
            '- ' + table_name,
            # '(' + str(n_db_columns) + '/' + str(n_spec_columns) + ' columns)',
        )

    # print missing
    missing_tables = get_missing_tables(
        db_metadata=db_metadata,
        spec_metadata=spec_metadata,
    )
    missing_from_db = missing_tables['missing_from_db']
    missing_from_spec = missing_tables['missing_from_spec']
    print()
    toolstr.print_header('Missing Tables')
    print('- tables missing from db (' + str(len(missing_from_db)) + ')')
    if len(missing_from_db) == 0:
        print('    [none]')
    else:
        for table in missing_from_db:
            print('    -', table)
    print('- tables missing from spec (' + str(len(missing_from_spec)) + ')')
    if len(missing_from_spec) == 0:
        print('    [none]')
    else:
        for table in missing_from_spec:
            print('    -', table)

    if full:
        metadatas = {'spec': spec_metadata, 'db': db_metadata}
        for name, metadata in metadatas.items():
            print()
            toolstr.print_header(name + ' tables')
            for table_name, table_obj in spec_metadata.tables.items():
                print('-', table_name)
                for column_name, column in table_obj.columns.items():
                    print('    -', column_name, repr(column))


def print_row_counts(
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

    if len(db_metadata.tables.keys()) == 0:
        print('[no tables exist]')

    labels = ['table', 'n_rows']
    rows = []
    for name, table in db_metadata.tables.items():

        # get row count
        statement = sqlalchemy.select(sqlalchemy.func.count()).select_from(  # type: ignore
            table
        )

        row_count = conn.execute(statement).scalar()

        # print('-', table, '(' + str(row_count) + ' rows)')
        row = [table, row_count]
        rows.append(row)

    for name, table in spec_metadata.tables.items():
        if name not in db_metadata.tables:
            # print('- [missing]', name, 'table')
            row = [name, 'missing']
            rows.append(row)
    toolstr.print_table(rows=rows, labels=labels)


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
    if db_config['dbms'] == 'sqlite':
        return os.path.getsize(db_config['path'])
    elif db_config['dbms'] == 'postgresql':
        cmd = """psql --dbname {database} --user {username} -c "\l+" | grep {database} | awk -F "|" '{print $1} {print $7}'"""
        for key, value in db_config.items():
            token = '{' + key + '}'
            if token in cmd:
                if not isinstance(value, str):
                    raise Exception('value must be str')
                cmd = cmd.replace(token, value)
        output = subprocess.check_output(
            cmd, shell=True, universal_newlines=True
        )
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
