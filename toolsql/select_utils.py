from __future__ import annotations

from . import spec
from . import driver_utils


def select(
    *,
    #
    # query parameters
    sql: str | None = None,
    parameters: spec.SqlParameters | None = None,
    #
    # execution parameters
    conn: spec.Connection,
    #
    # output parameters
    single_row: bool = False,
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.SelectOutput:

    if single_row and output_format == 'cursor':
        raise Exception('cannot use single row when returning cursor')

    # build query
    if sql is None:
        raise NotImplementedError('must specify sql query')
    driver = driver_utils.get_driver(conn=conn)
    sql, parameters = driver.build_select_query(
        sql,
        parameters,
    )

    # execute query
    result = driver.select(
        conn=conn,
        sql=sql,
        parameters=parameters,
        output_format=output_format,
    )

    return result


async def async_select(
    *,
    #
    # query parameters
    sql: str | None = None,
    parameters: spec.SqlParameters | None = None,
    #
    # connection parameters
    conn: spec.Connection,
    #
    # output parameters
    single_row: bool = False,
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.AsyncSelectOutput:

    if single_row and output_format == 'cursor':
        raise Exception('cannot use single row when returning cursor')

    # build query
    if sql is None:
        raise NotImplementedError('must specify sql')
    driver = driver_utils.get_driver_of_conn(conn=conn)
    sql, parameters = driver.build_select_query(
        sql,
        parameters,
    )

    # execute query
    result = await driver.async_select(
        conn=conn,
        sql=sql,
        parameters=parameters,
        output_format=output_format,
    )

    return result

