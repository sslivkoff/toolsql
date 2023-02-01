import conf.conf_tables as conf_tables

import pandas as pd
import polars as pl
import pytest

import toolsql


test_tables = conf_tables.get_test_tables()

# simple
simple = test_tables['simple']
simple_columns = list(simple['schema']['columns'].keys())

# pokemon
pokemon = test_tables['pokemon']
pokemon_columns = list(pokemon['schema']['columns'].keys())


_select_queries = {
    'SELECT * FROM simple': [
        {'select_kwargs': {'output_format': 'tuple'}, 'output': simple['rows']},
        {
            'select_kwargs': {'output_format': 'dict'},
            'output': [
                dict(zip(simple_columns, datum)) for datum in simple['rows']
            ],
        },
        {
            'select_kwargs': {'output_format': 'polars'},
            'output': pl.DataFrame(simple['rows'], columns=simple_columns),
        },
        {
            'select_kwargs': {'output_format': 'pandas'},
            'output': pd.DataFrame(simple['rows'], columns=simple_columns),
        },
    ],
    # 'SELECT * FROM pokemon': [
    #     {
    #         'select_kwargs': {
    #             'output_format': 'tuple',
    #             'raw_column_types': {'all_types': 'JSON'},
    #         },
    #         'output': pokemon['rows'],
    #     },
    #     {
    #         'select_kwargs': {
    #             'output_format': 'dict',
    #             'raw_column_types': {'all_types': 'JSON'},
    #         },
    #         'output': [
    #             dict(zip(pokemon_columns, datum)) for datum in pokemon['rows']
    #         ],
    #     },
    #     {
    #         'select_kwargs': {
    #             'output_format': 'polars',
    #             'raw_column_types': {'all_types': 'JSON'},
    #         },
    #         'output': pl.DataFrame(pokemon['rows'], columns=pokemon_columns),
    #     },
    #     {
    #         'select_kwargs': {
    #             'output_format': 'pandas',
    #             'raw_column_types': {'all_types': 'JSON'},
    #         },
    #         'output': pd.DataFrame(pokemon['rows'], columns=pokemon_columns),
    #     },
    # ],
}

select_queries = [
    {
        'sql': sql,
        'select_kwargs': output_set['select_kwargs'],
        'target_result': output_set['output'],
    }
    for sql in _select_queries.keys()
    for output_set in _select_queries[sql]
]


@pytest.fixture(params=select_queries)
def select_query(request):
    return request.param


def test_sync_select(sync_read_conn_db_config, select_query, helpers):

    sql = select_query['sql']
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']

    with toolsql.connect(sync_read_conn_db_config) as conn:
        result = toolsql.raw_select(sql=sql, conn=conn, **select_kwargs)

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_no_context(
    sync_read_conn_db_config, select_query, helpers
):

    sql = select_query['sql']
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    conn = toolsql.connect(sync_read_conn_db_config, as_context=False)

    try:
        result = toolsql.raw_select(sql=sql, conn=conn, **select_kwargs)
    finally:
        conn.close()

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_bare_conn(sync_read_bare_db_config, select_query, helpers):

    sql = select_query['sql']
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    result = toolsql.raw_select(
        sql=sql, conn=sync_read_bare_db_config, **select_kwargs
    )
    helpers.assert_results_equal(result=result, target_result=target_result)


#
# # async
#


async def test_async_select(async_read_conn_db_config, select_query, helpers):
    sql = select_query['sql']
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    async with toolsql.async_connect(async_read_conn_db_config) as conn:
        result = await toolsql.async_raw_select(
            sql=sql, conn=conn, **select_kwargs
        )

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select_no_context(
    async_read_conn_db_config, select_query, helpers
):
    sql = select_query['sql']
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    conn = await (
        toolsql.async_connect(async_read_conn_db_config, as_context=False)
    )

    try:
        result = await toolsql.async_raw_select(
            sql=sql, conn=conn, **select_kwargs
        )
    finally:
        await conn.close()

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select_bare_conn(
    async_read_bare_db_config, select_query, helpers
):
    sql = select_query['sql']
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']

    result = await toolsql.async_raw_select(
        sql=sql, conn=async_read_bare_db_config, **select_kwargs
    )

    helpers.assert_results_equal(result=result, target_result=target_result)

