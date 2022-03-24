from __future__ import annotations

import typing

import sqlalchemy  # type: ignore


SAEngine = sqlalchemy.engine.Engine
SAConnection = sqlalchemy.engine.Connection

SAColumn = sqlalchemy.sql.schema.Column
SARow = sqlalchemy.engine.Row
SATable = sqlalchemy.sql.schema.Table
SAMetadata = sqlalchemy.sql.schema.MetaData

SASelectStatement = sqlalchemy.sql.expression.Select
SAInsertStatement = sqlalchemy.sql.expression.Insert
SAUpdateStatement = sqlalchemy.sql.expression.Update
SADeleteStatement = sqlalchemy.sql.expression.Delete
SAStatement = typing.Union[
    sqlalchemy.sql.expression.Select,
    sqlalchemy.sql.expression.Insert,
    sqlalchemy.sql.expression.Update,
    sqlalchemy.sql.expression.Delete,
]
