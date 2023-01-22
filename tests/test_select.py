import numpy as np
import polars as pl
import pandas as pd

import toolsql


def _assert_equal(result, target_result):
    if isinstance(target_result, pl.DataFrame):
        assert target_result.frame_equal(result)
    elif isinstance(target_result, pd.DataFrame):
        assert np.all(target_result.columns.values == result.columns.values)
        for name, column in target_result.items():
            assert np.all(column.values == result[name].values)
    else:
        assert result == target_result


def test_sync_driver_select(sync_db_config, select_query):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']

    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(sql=sql, conn=conn, output_format=output_format)

    _assert_equal(result=result, target_result=target_result)


def test_sync_driver_select_no_context(sync_db_config, select_query):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    conn = toolsql.connect(sync_db_config, as_context=False)

    try:
        result = toolsql.select(sql=sql, conn=conn, output_format=output_format)
    finally:
        conn.close()

    _assert_equal(result=result, target_result=target_result)


async def test_async_driver_select(async_db_config, select_query):
    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    async with toolsql.async_connect(async_db_config) as conn:
        result = await toolsql.async_select(
            sql=sql, conn=conn, output_format=output_format
        )

    _assert_equal(result=result, target_result=target_result)


async def test_async_driver_select_no_context(async_db_config, select_query):
    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    conn = await toolsql.async_connect(async_db_config, as_context=False)

    try:
        result = await toolsql.async_select(
            sql=sql, conn=conn, output_format=output_format
        )
    finally:
        await conn.close()

    _assert_equal(result=result, target_result=target_result)

