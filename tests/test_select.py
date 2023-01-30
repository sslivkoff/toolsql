import conf.conf_tables as conf_tables

import pandas as pd
import polars as pl
import pytest

import toolsql


test_tables = conf_tables.get_test_tables()
simple_table = test_tables['simple']
schema = simple_table['schema']
simple_columns = list(schema['columns'].keys())
_select_queries = {
    'SELECT * FROM simple': {
        'tuple': simple_table['rows'],
        'dict': [
            dict(zip(simple_columns, datum)) for datum in simple_table['rows']
        ],
        'polars': pl.DataFrame(simple_table['rows'], columns=simple_columns),
        'pandas': pd.DataFrame(simple_table['rows'], columns=simple_columns),
    },
}
select_queries = [
    {'sql': sql, 'output_format': output_format, 'target_result': target_result}
    for sql in _select_queries.keys()
    for output_format, target_result in _select_queries[sql].items()
]


@pytest.fixture(params=select_queries)
def select_query(request):
    return request.param


def test_sync_select(sync_read_conn_db_config, select_query, helpers):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']

    with toolsql.connect(sync_read_conn_db_config) as conn:
        result = toolsql.raw_select(sql=sql, conn=conn, output_format=output_format)

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_no_context(
    sync_read_conn_db_config, select_query, helpers
):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    conn = toolsql.connect(sync_read_conn_db_config, as_context=False)

    try:
        result = toolsql.raw_select(sql=sql, conn=conn, output_format=output_format)
    finally:
        conn.close()

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_bare_conn(sync_read_bare_db_config, select_query, helpers):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    result = toolsql.raw_select(
        sql=sql, conn=sync_read_bare_db_config, output_format=output_format
    )
    helpers.assert_results_equal(result=result, target_result=target_result)


#
# # async
#

async def test_async_select(async_read_conn_db_config, select_query, helpers):
    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    async with toolsql.async_connect(async_read_conn_db_config) as conn:
        result = await toolsql.async_raw_select(
            sql=sql, conn=conn, output_format=output_format
        )

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select_no_context(
    async_read_conn_db_config, select_query, helpers
):
    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    conn = await (toolsql.async_connect(
        async_read_conn_db_config, as_context=False
    ))

    try:
        result = await toolsql.async_raw_select(
            sql=sql, conn=conn, output_format=output_format
        )
    finally:
        await conn.close()

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select_bare_conn(
    async_read_bare_db_config, select_query, helpers
):
    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']

    result = await toolsql.async_raw_select(
        sql=sql, conn=async_read_bare_db_config, output_format=output_format
    )

    helpers.assert_results_equal(result=result, target_result=target_result)

