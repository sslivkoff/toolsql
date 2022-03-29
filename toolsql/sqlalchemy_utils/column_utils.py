from __future__ import annotations

import typing

import sqlalchemy  # type: ignore

from .. import spec


def get_column_typemap() -> dict[str, typing.Any]:
    """return conversions of types into sqlalchemy types"""
    return {
        'Boolean': sqlalchemy.Boolean,
        'Integer': sqlalchemy.Integer,
        'BigInteger': sqlalchemy.BigInteger,
        'Float': sqlalchemy.Float,
        'Text': sqlalchemy.Text,
        'UUID': sqlalchemy.Text,
        'Timestamp': sqlalchemy.TIMESTAMP(timezone=True),
        'IP': sqlalchemy.Text,
        'JSON': sqlalchemy.JSON,
    }


def create_column_object_from_schema(
    column_schema: spec.ColumnSpec,
) -> spec.SAColumn:

    # add column properties
    args = []
    kwargs = {}
    if 'fk_table' in column_schema:
        args.append(_create_foreign_key(column_schema))
    if 'primary' in column_schema and column_schema['primary']:
        kwargs['primary_key'] = True
    if 'null' in column_schema and not column_schema['null']:
        kwargs['nullable'] = False
    if 'default' in column_schema:
        default = column_schema['default']
        if isinstance(default, bool):
            default = str(default).upper()
        elif isinstance(default, (int, float)):
            default = str(default)
        elif isinstance(default, str):
            pass
        else:
            raise Exception(
                'unsupported type of default: ' + str(type(default))
            )
        kwargs['server_default'] = default
    if 'unique' in column_schema:
        kwargs['unique'] = column_schema['unique']
    if column_schema.get('created_time'):
        # kwargs['default'] = datetime.datetime.utcnow
        kwargs['server_default'] = sqlalchemy.func.now()
    if column_schema.get('modified_time'):
        # kwargs['onupdate'] = datetime.datetime.utcnow
        kwargs['server_onupdate'] = sqlalchemy.func.now()
    if 'index' in column_schema:
        kwargs['index'] = column_schema['index']

    # create column
    typemap = get_column_typemap()
    column_type = typemap[column_schema['type']]
    column = sqlalchemy.Column(column_schema['name'], column_type, *args, **kwargs)

    return column


def _create_foreign_key(column_spec: spec.ColumnSpec) -> sqlalchemy.ForeignKey:
    """create sqlalchemy foreign key object from a column specification"""
    fk_id = column_spec['fk_table'] + '.' + column_spec['fk_column']
    fk_kwargs = {}
    if 'on_delete' in column_spec:
        fk_kwargs['ondelete'] = column_spec['on_delete'].upper()
    fk = sqlalchemy.ForeignKey(fk_id, **fk_kwargs)
    return fk

