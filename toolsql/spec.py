contenttypes = {
    'SQLConfig': {
        'dbms': 'Text',
        'path': 'Text',
        'engine': 'Text',
        'hostname': 'Text',
        'port': 'Integer',
        'database': 'Text',
        'username': 'Text',
        'password': 'Text',
        'socket': 'Text',
        'socket_dir': 'Text',
        'timeout': 'Text',
        'pool_timeout': 'Number',
    },
    'SQLMigrateConfig': {'migrate_root': 'Text', 'db_config': 'SQLConfig'},
    'SQLDBSchema': {'tables': {'SQLTableName': 'SQLTableSpec'}},
    'SQLTableSpec': {
        'columns': {'SQLColumnName': 'SQLColumn'},
        'constraints': ['SQLTableConstraint'],
        'indices': ['SQLTableIndex'],
        'column_order': ['SQLColumnName'],
    },
    'SQLColumn': {
        'type': 'Primal',
        'inner_type': 'Primal',  # for use by array
        'fk_table': 'SQLTableName',
        'fk_column': 'SQLColumnName',
        'unique': 'Boolean',  # for creating unique index
        'primary': 'Boolean',  # for use as primary key
        'null': 'Boolean',  # for whether column can be null
        'created_time': 'Boolean',  # timestamp of creation time
        'modified_time': 'Boolean',  # timestamp of creation time
        'default': 'Any',  # default value
        'length': 'Integer',  # for use by Text columns
        'virtual': 'Boolean',  # whether or not to actually create column in table
        'on_delete': 'SQLDeleteOption',  # what to do when foreign key deleted
        'index': 'Boolean',  # whether to create an index for column
    },
    'SQLTableName': 'Text',
    'SQLColumnName': 'Text',
    'SQLDeleteOption': [
        'cascade',
        'set null',
        'set default',
        'restrict',
        'no action',
    ],
    'SQLTableConstraint': {
        'constrainttype': ['unique'],
        'columns': ['SQLColumnName'],
    },
    'SQLTableIndex': {
        'name': 'Text',
        'columns': ['SQLColumnName'],
        'unique': 'Boolean',
    },
    'SQLQueryFilter': {
        'row_id': 'Any',
        'row_ids': ['Any'],
        'where_equals': {'SQLColumnName': 'Any'},
        'where_in': {'SQLColumnName': ['Any']},
        'where_foreign_row_equals': {'SQLColumnName': {'SQLColumnName': 'Any'}},
        'where_start_of': {'SQLColumnName': 'Text'},
        'filters': ['SQLAlchemyStatementObject'],
        'filter_by': {'Text': 'Any'},
        'order_by': ['SQLColumnOrder', ['SQLColumnOrder']],
        'only_columns': ['Text', 'SQLAlchemyColumnObject'],
    },
    'SQLColumnOrderMap': {'column': 'Text', 'order': ['descending', 'ascending']},
    'SQLColumnOrder': [
        'Text',
        'SQLColumnOrderMap',
        'SQLAlchemyColumnOrderObject',
    ],
    'SQLAlchemyColumnOrderObject': 'Object',
    'SQLAlchemyColumnObject': 'Object',
    'SQLAlchemyStatementObject': 'Object',
    # 'SQLFilterMap': {
    #     # this is a WIP
    #     # would be nice if there were a straightforward way to make it nested
    #     # - e.g. with or's and and's
    #     'operator': {'<=', '<', '==', '>', '>='},
    #     'lhs': 'SQLColumnName',
    #     'rhs': 'SQLColumnName',
    # },
    'SQLReturnCount': ['one', 'all'],
    'SQLRowCount': ['exactly_one', 'at_least_one', 'at_most_one'],
}

