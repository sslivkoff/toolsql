from __future__ import annotations

import typing

from toolsql import spec


#
# # validation
#


def is_cast_type(cast_type: str) -> bool:
    import re

    return re.match(r'^[A-Za-z0-9_]+$', cast_type) is not None


def is_table_name(table_name: str) -> bool:
    import re

    return re.match(r'^[A-Za-z0-9_]+$', table_name) is not None


def is_column_name(column: str) -> bool:
    import re

    return re.match(r'^[A-Za-z0-9_]+$', column) is not None


def is_function_call(column: str) -> bool:

    if column == 'COUNT(*)':
        return True

    elif column.startswith('COUNT(DISTINCT ') and column[-1] == ')':
        s = slice(len('COUNT(DISTINCT '), -1)
        return is_column_name(column[s])

    else:
        import re

        return re.match(r'^[A-Za-z0-9_]+\([A-Za-z0-9_]*\)$', column) is not None


def get_table_name(table: str | spec.TableSchema) -> str:

    if isinstance(table, str):
        table_name = table
    elif isinstance(table, dict):
        table_name = table['name']
    else:
        raise Exception('invalid table format: ' + str(type(table)))

    if not is_table_name(table_name):
        raise Exception('not a valid table name')

    return table_name


#
# # statement processing
#


def statement_to_single_line(sql: str) -> str:
    import re

    # https://stackoverflow.com/a/1546245
    return re.sub('[\n\t ]{2,}', ' ', sql).strip()


def get_dialect_placeholder(dialect: spec.Dialect) -> str:
    if dialect == 'postgresql':
        return '%s'
    elif dialect == 'sqlite':
        return '?'
    else:
        raise Exception('unknown dialect: ' + str(dialect))


#
# # statement creation
#


def build_columns_expression(
    columns: spec.ColumnsExpression | None,
    dialect: spec.Dialect,
    *,
    distinct: bool = False,
) -> str:

    if columns is None:
        columns_str = '*'
    else:
        columns_str = _columns_expression_to_str(columns, dialect=dialect)

    if distinct:
        return 'DISTINCT ' + columns_str
    else:
        return columns_str


def _columns_expression_to_str(
    columns: spec.ColumnsExpression,
    dialect: spec.Dialect,
) -> str:

    column_strs = []
    if isinstance(columns, (tuple, list)):
        for column in columns:
            column_str = _column_expression_to_str(column, dialect=dialect)
            column_strs.append(column_str)
    elif isinstance(columns, dict):
        for column_name, column_spec in columns.items():
            if isinstance(column_spec, str):
                new_column: spec.ColumnExpressionDict = dict(
                    column=column_name, alias=column_spec
                )
            elif isinstance(column_spec, dict):
                new_column = dict(column_spec, column=column_name)  # type: ignore
            else:
                raise Exception('invalid column specification')
            column_str = _column_expression_to_str(
                column=new_column,
                dialect=dialect,
            )
            column_strs.append(column_str)
    else:
        raise Exception('invalid format for columns')

    return ', '.join(column_strs)


def _column_expression_to_str(
    column: spec.ColumnExpression,
    dialect: spec.Dialect,
) -> str:
    """

    common sql functions: COUNT, MIN, MAX, AVG, SUM

    sqlite functions
    - core https://www.sqlite.org/lang_corefunc.html
    - date/time https://www.sqlite.org/lang_datefunc.html
    - aggregation https://www.sqlite.org/lang_aggfunc.html
    - window https://www.sqlite.org/windowfunctions.html#biwinfunc
    - math https://www.sqlite.org/lang_mathfunc.html
    - json https://www.sqlite.org/json1.html

    postgresql
    - all https://www.postgresql.org/docs/current/functions.html
    - strings https://www.postgresql.org/docs/current/functions-string.html
    """

    if isinstance(column, str):
        if is_column_name(column) or is_function_call(column):
            return column
        else:
            raise Exception('not a valid column expression: ' + str(column))

    elif isinstance(column, dict):

        alias = column.get('alias')

        column_name = column.get('column')
        if (
            column_name is not None
            and not is_column_name(column_name)
            and not is_function_call(column_name)
        ):
            raise Exception('is not valid column name: ' + str(column_name))

        encode = column.get('encode')
        if encode is not None:

            if column_name is None:
                raise Exception('must specify column_name to be encoded')
            if encode in ['raw_hex', 'prefix_hex']:
                if dialect == 'sqlite':
                    column_str = 'lower(hex(' + column_name + '))'
                elif dialect == 'postgresql':
                    column_str = 'encode(' + column_name + "::bytea, 'hex')"
                else:
                    raise Exception('unknown dialect: ' + str(dialect))

                if encode == 'prefix_hex':
                    column_str = "'0x' || " + column_str
            else:
                raise Exception('unknown encode format')

            if alias is None:
                alias = column_name

        elif column_name is not None:
            column_str = column_name
        else:
            raise Exception('must specify')

        # cast
        cast = column.get('cast')
        if cast is not None:
            if not is_cast_type(cast):
                raise Exception('is not valid cast name: ' + str(column_name))
            column_str = 'CAST(' + column_str + ' AS ' + cast + ')'

        # add alias
        if alias is not None:
            if is_column_name(alias):
                column_str = column_str + ' AS ' + alias
            else:
                raise Exception('invalid name for alias: ' + str(alias))

        return column_str

    else:
        raise Exception('unknown column format')


def _where_clause_to_str(
    *,
    dialect: spec.Dialect,
    where_equals: typing.Mapping[str, typing.Any] | None,
    where_gt: typing.Mapping[str, typing.Any] | None,
    where_gte: typing.Mapping[str, typing.Any] | None,
    where_lt: typing.Mapping[str, typing.Any] | None,
    where_lte: typing.Mapping[str, typing.Any] | None,
    where_like: typing.Mapping[str, str] | None,
    where_ilike: typing.Mapping[str, str] | None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None,
    where_or: typing.Sequence[spec.WhereGroup] | None,
    table: str | spec.TableSchema | None,
) -> tuple[str, tuple[typing.Any, ...]]:

    subclauses, parameters = _where_filters_to_str(
        dialect=dialect,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        where_or=where_or,
        table=table,
    )

    # finalize
    if len(subclauses) > 0:
        return 'WHERE ' + ' AND '.join(subclauses), tuple(parameters)
    else:
        return '', tuple()


def _where_filters_to_str(
    *,
    dialect: spec.Dialect,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
    table: str | spec.TableSchema | None,
) -> tuple[list[str], list[typing.Any]]:

    if isinstance(table, dict):
        column_types = {
            column['name']: column['type'].upper()
            for column in table['columns']
        }
    else:
        column_types = {}

    placeholder = get_dialect_placeholder(dialect)
    subclauses = []
    parameters = []

    for item, symbol in [
        (where_equals, ' = '),
        (where_gt, ' > '),
        (where_gte, ' >= '),
        (where_lt, ' < '),
        (where_lte, ' <= '),
        (where_like, ' LIKE '),
        (where_ilike, ' ILIKE '),
    ]:
        if item is not None:
            for column_name, column_value in item.items():
                if not is_column_name(column_name):
                    raise Exception('not a valid column name')

                # handle dialect-specific operations
                if dialect == 'sqlite':
                    if symbol == ' ILIKE ':
                        # in sqlite LIKE Is case-insensitive by default
                        symbol = ' LIKE '
                    elif symbol == ' LIKE ':
                        raise NotImplementedError(
                            'case-sensitive LIKE for sqlite'
                        )

                # convert hex to binary
                if (
                    symbol == ' = '
                    and column_types.get(column_name) in spec.binary_columntypes
                ):
                    column_value = _convert_hex_to_bytes(column_value)
                elif (
                    symbol == ' = '
                    and isinstance(table, str)
                    and _is_hex_str(column_value)
                ):
                    import warnings

                    warnings.warn(
                        'should provide full table schema in order to convert hex byte strs to binary'
                    )

                subclauses.append(column_name + symbol + placeholder)
                parameters.append(column_value)

    if where_in is not None:
        for column_name, column_value in where_in.items():

            if len(column_value) == 0:
                raise Exception('cannot use WHERE IN with empty list')

            # convert hex to binary
            if (
                column_types is not None
                and column_types.get(column_name) in spec.binary_columntypes
            ):
                column_value = [
                    _convert_hex_to_bytes(subvalue) for subvalue in column_value
                ]
            elif isinstance(table, str):
                for subvalue in column_value:
                    if _is_hex_str(subvalue):
                        import warnings

                        warnings.warn(
                            'should provide full table schema in order to convert hex byte strs to binary'
                        )

            if not is_column_name(column_name):
                raise Exception('not a valid column name')
            multiplaceholder = ','.join([placeholder] * len(column_value))
            subclauses.append(column_name + ' IN (' + multiplaceholder + ')')
            for subitem in column_value:
                parameters.append(subitem)

    if where_or is not None and len(where_or) > 0:
        subsubclauses = []
        for sub_where_or in where_or:
            subsubclause, subparameters = _where_filters_to_str(
                dialect=dialect, table=table, **sub_where_or
            )
            subsubclauses.append(' AND '.join(subsubclause))
            parameters.extend(subparameters)
        subclauses.append('(' + ' OR '.join(subsubclauses) + ')')

    return subclauses, parameters


def _is_hex_str(s: typing.Any) -> bool:
    if not isinstance(s, str):
        return False
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def _convert_hex_to_bytes(column_value: typing.Any) -> typing.Any:
    if isinstance(column_value, str):
        try:
            if column_value.startswith('0x'):
                return bytes.fromhex(column_value[2:])
            else:
                return bytes.fromhex(column_value)
        except ValueError:
            raise Exception('cannot use str argument for BLOB column type')
    else:
        return column_value


#
# # parameters
#


def populate_sql_parameters(
    sql: str,
    parameters: spec.ExecuteParams,
    dialect: spec.Dialect,
) -> str:
    # INCOMPLETE
    if dialect == 'sqlite' and isinstance(parameters, (list, tuple)):
        sql = sql.replace('?', "{}")
        formatted_parameters = []
        for parameter in parameters:
            if isinstance(parameter, int):
                formated_parameter = str(parameter)
            elif isinstance(parameter, str):
                formated_parameter = '"' + parameter + '"'
            elif isinstance(parameter, bytes):
                formated_parameter = "x'" + parameter.hex() + "'"
            else:
                raise NotImplementedError('parameter type: ' + str(type(parameter)))
            formatted_parameters.append(formated_parameter)
        sql = sql.format(*formatted_parameters)
        return sql
    elif dialect == 'postgresql':
        import psycopg

        if isinstance(parameters, (list, tuple)):
            formatted_parameters = [
                psycopg.sql.Literal(parameter).as_string(None)
                for parameter in parameters
            ]
            sql = sql.replace('%s', '{}').format(*formatted_parameters)
            return sql

    raise NotImplementedError()

