from __future__ import annotations

import aiosqlite

from toolsql import spec
from . import abstract_driver


class AiosqliteDriver(abstract_driver.AbstractDriver):
    name = 'aiosqlite'

    @classmethod
    def async_connect(
        cls,
        uri: str,
        as_context: bool,
        autocommit: bool,
    ) -> spec.AsyncConnection:
        path = uri.split('sqlite://')[1]

        if autocommit:
            return aiosqlite.connect(path, isolation_level=None)
        else:
            return aiosqlite.connect(path)

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor,
    ) -> tuple[str, ...] | None:
        if not isinstance(cursor, aiosqlite.Cursor):
            raise Exception('not an aiosqlite cursor')
        return tuple(item[0] for item in cursor.description)

