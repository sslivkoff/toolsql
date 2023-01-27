from __future__ import annotations

from toolsql import spec


def build_select_query(
    sql: str | None,
    parameters: spec.SqlParameters | None,
    dialect: str,
) -> tuple[str, spec.SqlParameters]:
    if sql is None:
        raise Exception()
    if parameters is None:
        parameters = tuple()
    return sql, parameters

