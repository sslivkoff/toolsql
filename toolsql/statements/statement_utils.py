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


def is_column_expression(column: str) -> bool:
    """
    currently checks for:
    - plain column names
    - capitalized aggregation functions (COUNT, MIN, MAX, AVG, SUM)
    """
    if is_column_name(column):
        return True
    elif column == 'COUNT(*)':
        return True
    else:
        for item in [
            'COUNT(',
            'COUNT(DISTINCT ',
            'MIN(',
            'MAX(',
            'AVG(',
            'SUM(',
        ]:
            if not column.startswith(item) or column[-1] != ')':
                continue
            column_name = column[len(item) : -1]
            if is_column_name(column_name):
                return True
        else:
            return False



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
) -> tuple[list[str], list[typing.Any]]:

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

                subclauses.append(column_name + symbol + placeholder)
                parameters.append(column_value)

    if where_in is not None:
        for column_name, column_value in where_in.items():
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
                dialect=dialect, **sub_where_or
            )
            subsubclauses.append(' AND '.join(subsubclause))
            parameters.extend(subparameters)
        subclauses.append('(' + ' OR '.join(subsubclauses) + ')')

    return subclauses, parameters

