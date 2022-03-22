from __future__ import annotations

import copy
import typing

from . import spec


def compile_shorthand_db_schema(
    db_schema: typing.Optional[spec.DBSchema] = None,
    tables_columns: typing.Optional[dict[str, spec.ColumnSpec]] = None,
    add_to_all_tables=None,
) -> spec.DBSchema:
    if db_schema is None:
        db_schema = {
            'tables': {
                table_name: {'columns': table_columns}
                for table_name, table_columns in tables_columns.items()
            }
        }
    else:
        db_schema = copy.deepcopy(db_schema)

    # within-table data
    for table_name, table_spec in db_schema['tables'].items():

        # add column order
        if table_spec.get('column_order') is None:
            table_spec['column_order'] = list(table_spec['columns'])

        # add primary key column
        primary_column = _get_primary_column(table_spec)
        if primary_column is None:
            column_name = table_name + '_id'
            table_spec['columns'][column_name] = {
                'primary': True,
                'type': 'Integer',
            }
            table_spec['column_order'].insert(0, column_name)

        # add items to table
        if add_to_all_tables is not None:
            for add_this in add_to_all_tables:
                if add_this == 'created_time':
                    if 'created_time' not in table_spec['columns']:
                        table_spec['columns']['created_time'] = {
                            'type': 'Timestamp',
                            'created_time': True,
                        }
                elif add_this == 'modified_time':
                    if 'modified_time' not in table_spec['columns']:
                        table_spec['columns']['modified_time'] = {
                            'type': 'Timestamp',
                            'modified_time': True,
                        }
                else:
                    raise Exception('unknown item to add: ' + str(add_this))

    # across-table data
    for table_name, table_spec in db_schema['tables'].items():

        # process foreign keys
        for column_name, column_spec in table_spec['columns'].items():
            if 'fk_table' in column_spec:
                fk_table_spec = db_schema['tables'][column_spec['fk_table']]
                fk_column_name = _get_primary_column(fk_table_spec)
                fk_column_spec = fk_table_spec['columns'][fk_column_name]
                if column_spec.get('type') is None:
                    column_spec['type'] = fk_column_spec['type']
                if column_spec.get('fk_column') is None:
                    column_spec['fk_column'] = fk_column_name

    return db_schema


def _get_primary_column(table_spec: spec.TableSpec) -> typing.Optional[str]:
    for column_name, column_spec in table_spec['columns'].items():
        if column_spec.get('primary'):
            return column_name
    return None


def verify_db_schema(db_schema: spec.DBSchema) -> None:

    for table_name, table_spec in db_schema['tables'].items():
        # has primary key

        for column_name, column_spec in table_spec['columns'].items():

            # has type
            if 'type' not in column_spec:
                raise Exception(
                    'must specify type on column "'
                    + column_name
                    + '" in table "'
                    + table_name
                    + '"'
                )

            #
            if not column_spec['type'][0].isupper():
                raise Exception(
                    'capitalize type name of column "'
                    + column_name
                    + '" in table "'
                    + table_name
                    + '"'
                )

            # foreign keys
            if 'fk_table' in column_spec:
                for key in ['fk_column', 'on_delete']:
                    if key not in column_spec:
                        raise Exception(
                            'foreign key "'
                            + column_name
                            + '" in table "'
                            + table_name
                            + '" should specify '
                            + key
                        )

