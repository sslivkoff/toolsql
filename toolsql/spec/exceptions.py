from __future__ import annotations

import typing


class CannotConnect(Exception):
    pass


class TableDoesNotExist(Exception):
    pass


def convert_exception(e: Exception, context: typing.Any = None) -> Exception:
    name = type(e).__name__
    module = type(e).__module__
    print('ARGS', e.args)
    if name == 'UndefinedTable' and module == 'psycopg.errors':
        return TableDoesNotExist(context)
    elif name == 'OperationalError' and module == 'sqlite3':
        if (
            len(e.args) > 0
            and isinstance(e.args[0], str)
            and e.args[0].startswith('no such table')
        ):
            return TableDoesNotExist(context)
    elif name == 'RuntimeError' and module == 'builtins':
        if (
            len(e.args) > 0
            and isinstance(e.args[0], str)
            and (
                (
                    e.args[0].startswith('db error: ERROR: relation "')
                    and e.args[0].endswith('" does not exist')
                )
                or (
                    e.args[0].startswith('no such table: ')
                )
            )
        ):
            return TableDoesNotExist(context)

    return e

