from __future__ import annotations

import typing
from typing_extensions import TypedDict


DatabaseSystem = typing.Literal['sqlite', 'postgres']


class SQLConfig(TypedDict):
    dbms: DatabaseSystem
    path: str
    engine: str
    hostname: str
    port: int
    database: str
    username: str
    password: str
    socket: str
    socket_dir: str
    timeout: typing.Union[int, float]
    pool_timeout: typing.Union[int, float]


class SQLMigrateConfig(TypedDict):
    migrate_root: str
    db_config: SQLConfig

