from __future__ import annotations

import typing
from typing_extensions import Literal
from toolsql import spec


#
# # validation
#


def is_column_name(column: str) -> bool:
    import re

    return re.match(r'^[A-Za-z0-9_]+$', column) is not None


def is_table_name(table_name: str) -> bool:
    import re

    return re.match(r'^[A-Za-z0-9_]+$', table_name) is not None


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
) -> tuple[str, tuple[typing.Any, ...]]:

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

    # finalize
    if len(subclauses) > 0:
        return 'WHERE ' + ' AND '.join(subclauses), tuple(parameters)
    else:
        return '', tuple()


def _wrap_json_columns(
    *,
    rows: spec.ExecuteManyParams | None = None,
    dialect: Literal['sqlite', 'postgresql'],
) -> spec.ExecuteManyParams | None:
    if rows is not None:
        new_rows: list[typing.Any] | None = None
        for r, row in enumerate(rows):
            new_row: typing.MutableSequence[typing.Any] | typing.MutableMapping[
                str, typing.Any
            ] | None = None
            if isinstance(row, (list, tuple)):
                if any(isinstance(cell, (dict, list, tuple)) for cell in row):
                    new_row = [
                        cell
                        if isinstance(cell, (dict, list, tuple))
                        else _wrap_json_cell(cell, dialect)
                        for cell in row
                    ]
            elif isinstance(row, dict):
                for key, value in row.items():
                    if isinstance(value, (dict, list, tuple)):
                        if new_row is None:
                            new_row = row.copy()
                        new_row[key] = _wrap_json_cell(value, dialect)
            else:
                raise Exception('unknown row')

            if new_row is not None:
                if new_rows is None:
                    new_rows = list(rows)
                new_rows[r] = new_row

        if new_rows is not None:
            rows = new_rows

    return rows


def _wrap_json_cell(item: typing.Any, dialect: spec.Dialect) -> typing.Any:
    if dialect == 'postgresql':
        from psycopg.types.json import Jsonb

        return Jsonb(item)
    elif dialect == 'sqlite':
        import json

        return json.dumps(item)
    else:
        raise Exception('unknown dialect: ' + str(dialect))

