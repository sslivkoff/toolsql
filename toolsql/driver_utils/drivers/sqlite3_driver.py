from __future__ import annotations

import sqlite3

from toolsql import spec
from . import abstract_driver


class Sqlite3Driver(abstract_driver.AbstractDriver):
    name = 'sqlite3'

    @classmethod
    def connect(cls, uri: str, *, as_context: bool = True) -> spec.Connection:
        path = uri.split('sqlite://')[1]
        return sqlite3.connect(path)

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor,
    ) -> tuple[str, ...] | None:
        if not isinstance(cursor, sqlite3.Cursor):
            raise Exception('not a sqlite3 cursor')
        return tuple(item[0] for item in cursor.description)

