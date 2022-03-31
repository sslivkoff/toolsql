from __future__ import annotations

import typing
from typing_extensions import TypedDict, Literal


DatabaseSystem = Literal['sqlite', 'postgresql']


class DBConfig(TypedDict, total=False):
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


class MigrateConfig(TypedDict):
    migrate_root: str
    db_config: DBConfig

