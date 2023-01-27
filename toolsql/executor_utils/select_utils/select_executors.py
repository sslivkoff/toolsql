from __future__ import annotations

from toolsql import conn_utils
from toolsql import dialect_utils
from toolsql import spec
from . import dbapi_selection
from . import connectorx_selection


def select(
    *,
    #
    # query parameters
    sql: str | None = None,
    parameters: spec.SqlParameters | None = None,
    #
    # execution parameters
    conn: spec.Connection | str,
    #
    # output parameters
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.SelectOutput:

    # build query
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_select_query(
        sql,
        parameters,
        dialect=dialect,
    )

    # execute query
    driver = conn_utils.get_conn_driver(conn=conn)
    if driver.name == 'connectorx':
        if not isinstance(conn, str):
            raise Exception('connectorx must use str conn')
        return connectorx_selection._select_connectorx(
            sql=sql,
            conn=conn,
            output_format=output_format,
        )
    else:
        if isinstance(conn, str):
            raise Exception('conn not initialized')
        return dbapi_selection._select_dbapi(
            sql=sql,
            parameters=parameters,
            conn=conn,
            output_format=output_format,
            driver=driver,
        )


async def async_select(
    *,
    #
    # query parameters
    sql: str | None = None,
    parameters: spec.SqlParameters | None = None,
    #
    # connection parameters
    conn: spec.AsyncConnection | str,
    #
    # output parameters
    single_row: bool = False,
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.AsyncSelectOutput:

    # build query
    if sql is None:
        raise NotImplementedError('must specify sql')
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_select_query(
        sql,
        parameters,
        dialect=dialect,
    )

    # execute query
    driver = conn_utils.get_conn_driver(conn=conn)
    if driver.name == 'connectorx':
        if not isinstance(conn, str):
            raise Exception('connectorx must use str conn')
        return await connectorx_selection._async_select_connectorx(
            sql=sql,
            conn=conn,
            output_format=output_format,
        )
    else:
        if isinstance(conn, str):
            raise Exception('conn not initialized')
        return await dbapi_selection._async_select_dbapi(
            sql=sql,
            parameters=parameters,
            conn=conn,
            output_format=output_format,
            driver=driver,
        )

