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

polars_pokemon = pl.DataFrame(pokemon['rows'], columns=pokemon_columns)
polars_pokemon = polars_pokemon.with_column(
    pl.Series(
        values=[list(item) for item in polars_pokemon['all_types']],
        dtype=pl.Object,
        name='all_types',
    )
)

select_queries = [
    #
    # simple table
    {
        'select_kwargs': {'table': 'simple', 'output_format': 'tuple'},
        'target_result': simple['rows'],
    },
    {
        'select_kwargs': {'table': 'simple', 'output_format': 'dict'},
        'target_result': [
            dict(zip(simple_columns, datum)) for datum in simple['rows']
        ],
    },
    {
        'select_kwargs': {'table': 'simple', 'output_format': 'polars'},
        'target_result': pl.DataFrame(simple['rows'], columns=simple_columns),
    },
    {
        'select_kwargs': {'table': 'simple', 'output_format': 'pandas'},
        'target_result': pd.DataFrame(simple['rows'], columns=simple_columns),
    },
    #
    # pokemon table
    {
        'select_kwargs': {'table': 'pokemon', 'output_format': 'tuple'},
        'target_result': pokemon['rows'],
    },
    {
        'select_kwargs': {'table': 'pokemon', 'output_format': 'dict'},
        'target_result': [
            dict(zip(pokemon_columns, datum)) for datum in pokemon['rows']
        ],
    },
    {
        'select_kwargs': {'table': 'pokemon', 'output_format': 'polars'},
        'target_result': polars_pokemon,
    },
    {
        'select_kwargs': {'table': 'pokemon', 'output_format': 'pandas'},
        'target_result': pd.DataFrame(pokemon['rows'], columns=pokemon_columns),
    },
]


@pytest.fixture(params=select_queries)
def select_query(request):
    return request.param


def test_sync_select(sync_read_conn_db_config, select_query, helpers):

    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']

    with toolsql.connect(sync_read_conn_db_config) as conn:
        result = toolsql.select(conn=conn, **select_kwargs)

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_no_context(
    sync_read_conn_db_config, select_query, helpers
):

    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    conn = toolsql.connect(sync_read_conn_db_config, as_context=False)

    try:
        result = toolsql.select(conn=conn, **select_kwargs)
    finally:
        conn.close()

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_bare_conn(sync_read_bare_db_config, select_query, helpers):

    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    result = toolsql.select(
        conn=sync_read_bare_db_config, **select_kwargs
    )
    helpers.assert_results_equal(result=result, target_result=target_result)


#
# # async
#


# async def test_async_select(async_read_conn_db_config, select_query, helpers):
#     sql = select_query['sql']
#     select_kwargs = select_query['select_kwargs']
#     target_result = select_query['target_result']
#     async with toolsql.async_connect(async_read_conn_db_config) as conn:
#         result = await toolsql.async_raw_select(
#             sql=sql, conn=conn, **select_kwargs
#         )

#     helpers.assert_results_equal(result=result, target_result=target_result)


# async def test_async_select_no_context(
#    async_read_conn_db_config, select_query, helpers
# ):
#    sql = select_query['sql']
#    select_kwargs = select_query['select_kwargs']
#    target_result = select_query['target_result']
#    conn = await (
#        toolsql.async_connect(async_read_conn_db_config, as_context=False)
#    )

#    try:
#        result = await toolsql.async_raw_select(
#            sql=sql, conn=conn, **select_kwargs
#        )
#    finally:
#        await conn.close()

#    helpers.assert_results_equal(result=result, target_result=target_result)


# async def test_async_select_bare_conn(
#    async_read_bare_db_config, select_query, helpers
# ):
#    sql = select_query['sql']
#    select_kwargs = select_query['select_kwargs']
#    target_result = select_query['target_result']

#    result = await toolsql.async_raw_select(
#        sql=sql, conn=async_read_bare_db_config, **select_kwargs
#    )

#    helpers.assert_results_equal(result=result, target_result=target_result)

