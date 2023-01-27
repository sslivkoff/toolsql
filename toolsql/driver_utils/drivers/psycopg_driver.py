from __future__ import annotations

import psycopg

from toolsql import conn_utils
from toolsql import spec
from . import abstract_driver


class PsycopgDriver(abstract_driver.AbstractDriver):
    name = 'psycopg'

    @classmethod
    def get_psycopg_conn_str(cls, target: str | spec.DBConfig) -> str:
        if isinstance(target, str):
            db_config = conn_utils.parse_uri(target)
        elif isinstance(target, dict):
            db_config = target
        else:
            raise Exception('invalid connection target')
        return 'dbname={database} user={username}'.format(**db_config)

    @classmethod
    def connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.Connection:
        connect_str = cls.get_psycopg_conn_str(uri)
        return psycopg.connect(connect_str, autocommit=autocommit)

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor | spec.AsyncCursor,
    ) -> tuple[str, ...] | None:
        description = cursor.description
        if description is None:
            return None
        else:
            return tuple(item.name for item in description)  # type: ignore

