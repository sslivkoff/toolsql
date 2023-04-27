from __future__ import annotations

import typing

from toolsql import schemas
from toolsql import spec
from .. import statement_utils


def build_create_table_statement(
    table: spec.TableSchema,
    *,
    if_not_exists: bool = False,
    single_line: bool = True,
    dialect: spec.Dialect,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_createtable.html
    - postgresql: https://www.postgresql.org/docs/current/sql-createtable.html
    """

    table = schemas.normalize_shorthand_table_schema(table)

    columns_str = _format_columns(columns=table['columns'], dialect=dialect)

    template = """
    CREATE TABLE {if_not_exists}
    {table_name}
    ( {columns} )
    """
    sql = template.format(
        table_name=table['name'],
        if_not_exists='IF NOT EXISTS' if if_not_exists else '',
        columns=columns_str,
    )

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql.rstrip()


def _format_columns(
    columns: typing.Sequence[spec.ColumnSchema],
    dialect: spec.Dialect,
) -> str:

    primary_columns = [
        column['name'] for column in columns if column.get('primary')
    ]
    composite_primary_key = len(primary_columns) > 1

    column_strs = [
        _format_column(
            column,
            dialect=dialect,
            composite_primary_key=composite_primary_key,
        )
        for column in columns
    ]

    if composite_primary_key:
        primary = 'PRIMARY KEY ({})'.format(', '.join(primary_columns))
        column_strs.append(primary)

    return ', '.join(column_strs)


def _format_column(
    column: spec.ColumnSchema,
    dialect: spec.Dialect,
    composite_primary_key: bool = False,
) -> str:

    columntype = schemas.convert_columntype_to_dialect(
        column['type'],
        dialect,
    )
    line = column['name'] + ' ' + columntype.upper()

    if column['primary'] and not composite_primary_key:
        line = line + ' PRIMARY KEY'

    if column['autoincrement']:
        if column['autoincrement'] == 'strict':
            if dialect == 'postgresql':
                line = line + ' GENERATED ALWAYS AS IDENTITY'
            elif dialect == 'sqlite':
                line = line + ' AUTOINCREMENT'
            else:
                raise Exception('invalid dialect: ' + str(dialect))
        else:
            if dialect == 'postgresql':
                line = line + ' GENERATED BY DEFAULT AS IDENTITY'
            elif dialect == 'sqlite':
                # this is already default behavior
                pass
            else:
                raise Exception('invalid dialect: ' + str(dialect))

    if not column['nullable']:
        line = line + ' NOT NULL'

    if column['unique']:
        line = line + ' UNIQUE'

    if column['default'] is not None:
        default = column['default']
        if isinstance(default, str):
            formatted_default = "'" + default + "'"
        elif isinstance(default, int):
            formatted_default = str(default)
        else:
            raise Exception('unknown format for default value: ' + str(type(default)))
        line = line + ' DEFAULT ' + formatted_default

    return line


def build_create_index_statement(
    table_schema: spec.TableSchema,
    column_name: str | None = None,
    *,
    column_names: typing.Sequence[str] | None = None,
    index_name: str | None = None,
    if_not_exists: bool = False,
    single_line: bool = True,
    dialect: spec.Dialect,
    unique: bool = False,
    nulls_equal: bool = False,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_createindex.html
    - postgresql: https://www.postgresql.org/docs/current/sql-createindex.html
    """

    if column_name is not None and column_names is not None:
        raise Exception('specify only column_name or column_names')
    elif column_name is not None:
        columns: typing.Sequence[str] = [column_name]
    elif column_names is not None:
        columns = column_names
    else:
        raise Exception('specify column_name or column_names')

    if nulls_equal:
        nullable_columns = {
            column['name']
            for column in table_schema['columns']
            if column['nullable']
        }
        column_types = {
            column['name']: column['type']
            for column in table_schema['columns']
            if column['nullable']
        }
        new_columns = []
        for column in columns:
            if (
                column in nullable_columns
                or not statement_utils.is_column_name(column)
            ):
                if dialect == 'postgresql':
                    function = 'COALESCE'
                elif dialect == 'sqlite':
                    function = 'IFNULL'
                else:
                    raise Exception('unknown dialect')
                column_type = column_types[column]
                if column_type == 'TEXT':
                    null_placeholder: typing.Any = "'!@#$NULL_PLACEHOLDER!@#$'"
                elif column_type in spec.integer_columntypes:
                    null_placeholder = str(-9999999999999)
                else:
                    raise Exception('could not determine null placeholder')
                new_columns.append(
                    function + "(" + column + ", " + str(null_placeholder) + ")"
                )
            else:
                new_columns.append(column)
        columns = new_columns

    if index_name is None:
        index_name = create_default_index_name(
            {
                'columns': column_names,
                'unique': unique,
                'nulls_equal': nulls_equal,
            },
            table=table_schema,
            column_name=column_name,
        )

    template = """
    CREATE {uniqueness} INDEX
    {if_not_exists}
    {index_name}
    ON
    {table_name}
    ( {columns} )
    """
    sql = template.format(
        uniqueness=('UNIQUE' if unique else ''),
        if_not_exists=('IF NOT EXISTS' if if_not_exists else ''),
        index_name=index_name,
        table_name=table_schema['name'],
        columns=', '.join(columns),
    )

    if single_line:
        sql = statement_utils.statement_to_single_line(sql)

    return sql


def create_default_index_name(
    index: typing.Mapping[str, typing.Any],
    table: str | spec.TableSchema,
    column_name: str | None = None,
) -> str:

    if isinstance(table, str):
        table_name = table
    elif isinstance(table, dict):
        table_name = table['name']
    else:
        raise Exception('unknown table format')

    if index['columns'] is None and column_name is not None:
        return 'index___' + table_name + '___' + column_name

    else:

        index_name = (
            'index___' + table_name + '___' + '__'.join(index['columns'])
        )
        index_name = index_name.lower()
        for char in '() -\'",!@#$':
            index_name = index_name.replace(char, '_')
        if len(index_name) > 40:
            import hashlib

            name_hash = hashlib.md5(index_name.encode()).hexdigest()
            index_name = 'index___' + table_name + '___' + name_hash
        if len(index_name) > 63:
            index_name = index_name[:63]
        return index_name


def build_all_table_schema_create_statements(
    table_schema: spec.TableSchema,
    *,
    dialect: spec.Dialect,
    if_not_exists: bool = False,
    single_line: bool = True,
) -> typing.Sequence[str]:

    create_table = build_create_table_statement(
        table_schema,
        dialect=dialect,
        if_not_exists=if_not_exists,
        single_line=single_line,
    )

    # single column indices
    create_index_statements = [
        build_create_index_statement(
            table_schema=table_schema,
            column_name=column['name'],
            dialect=dialect,
            if_not_exists=if_not_exists,
            single_line=single_line,
        )
        for column in table_schema['columns']
        if column['index']
    ]

    # multi column indices
    for index in table_schema['indices']:
        create_index_statement = build_create_index_statement(
            index_name=index['name'],
            table_schema=table_schema,
            column_names=index['columns'],
            unique=index['unique'],
            nulls_equal=index['nulls_equal'],
            dialect=dialect,
            if_not_exists=if_not_exists,
            single_line=single_line,
        )
        create_index_statements.append(create_index_statement)

    return [create_table] + create_index_statements

