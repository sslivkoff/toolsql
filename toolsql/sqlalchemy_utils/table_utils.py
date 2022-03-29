"""functions for interacting with sqlalchemy tables"""

from __future__ import annotations

import typing

import sqlalchemy  # type: ignore
import toolcache

from .. import spec
from . import column_utils
from . import metadata_utils


def get_table_primary_key(table: spec.SATable) -> str:
    primary_keys = list(table.primary_key.columns)
    if len(primary_keys) != 1:
        raise Exception('multi column primary key')
    primary_key = primary_keys[0]
    return primary_key.name


def create_table_object_from_schema(
    table_name: str,
    table_schema: spec.TableSpec,
    metadata: spec.SAMetadata,
) -> spec.SATable:
    """create sqlalchemy table object from schema specification"""

    # create columns
    column_specs = table_schema['columns']
    table_items = []
    for column_spec in column_specs:
        if column_spec.get('virtual'):
            continue
        column = column_utils.create_column_object_from_schema(
            column_schema=column_spec,
        )
        table_items.append(column)

    # create constraints
    constraint_specs = table_schema.get('constraints')
    if constraint_specs is not None:
        for constraint_spec in constraint_specs:
            constrainttype = constraint_spec['constrainttype']
            if constrainttype == 'unique':
                columns = constraint_spec['columns']
                constraint = sqlalchemy.UniqueConstraint(*columns)
            else:
                raise Exception(
                    'unknown constraint type: ' + str(constrainttype)
                )
            table_items.append(constraint)

    # create indices
    index_specs = table_schema.get('indices')
    if index_specs is not None:
        raise NotImplementedError('not tested')
        for index_spec in index_specs:
            name = index_spec['name']
            args = index_spec['columns']
            kwargs = {}
            if index_spec.get('unique'):
                kwargs['unique'] = True
            index = sqlalchemy.Index(name, *args, **kwargs)
            table_items.append(index)

    table = sqlalchemy.Table(table_name, metadata, *table_items)

    return table


@toolcache.cache(cachetype='memory')
def create_table_object_from_db(
    *,
    table_name: str,
    metadata: spec.SAMetadata | None = None,
    engine: spec.SAEngine | None = None,
    conn: spec.SAConnection | None = None,
    db_config: spec.DBConfig | None = None,
) -> spec.SATable:
    """create sqlalchemy table object reflecting current database"""
    if metadata is None:
        if engine is None and conn is None:
            raise Exception('must specify metadata, engine, or conn')
        metadata = metadata_utils.create_metadata_object_from_db(
            engine=engine,
            conn=conn,
            db_config=db_config,
        )
    table = metadata.tables[table_name]
    return table

