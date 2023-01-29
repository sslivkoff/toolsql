from __future__ import annotations

from toolsql import spec


def statement_to_single_line(sql: str) -> str:
    import re

    # https://stackoverflow.com/a/1546245
    return re.sub('[\n\t ]{2,}', ' ', sql).strip()


def is_column_name(column: str) -> bool:
    import re

    return re.match(r'^[A-Za-z0-9_]+$', column) is not None


def is_table_name(table_name: str) -> bool:
    import re

    return re.match(r'^[A-Za-z0-9_]+$', table_name) is not None


def get_dialect_placeholder(dialect: spec.Dialect) -> str:
    if dialect == 'postgresql':
        return '%s'
    elif dialect == 'sqlite':
        return '?'
    else:
        raise Exception('unknown dialect: ' + str(dialect))

