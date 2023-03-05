import conf.conf_tables as conf_tables

import pandas as pd
import polars as pl
import pytest

import toolsql


test_tables = conf_tables.get_test_tables()

# simple
simple = test_tables['simple']
simple_columns = list(simple['schema']['columns'].keys())
simple_schema = toolsql.normalize_shorthand_table_schema(simple['schema'])

# pokemon
pokemon = test_tables['pokemon']
pokemon_columns = list(pokemon['schema']['columns'].keys())

polars_pokemon = pl.DataFrame(pokemon['rows'], schema=pokemon_columns)
polars_pokemon = polars_pokemon.with_columns(
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
        'select_kwargs': {'table': simple_schema, 'output_format': 'tuple'},
        'target_result': simple['rows'],
    },
    {
        'select_kwargs': {'table': simple_schema, 'output_format': 'dict'},
        'target_result': [
            dict(zip(simple_columns, datum)) for datum in simple['rows']
        ],
    },
    {
        'select_kwargs': {'table': simple_schema, 'output_format': 'polars'},
        'target_result': pl.DataFrame(
            simple['rows'], schema=simple_columns, orient='row'
        ),
    },
    {
        'select_kwargs': {'table': simple_schema, 'output_format': 'pandas'},
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

parameterized_select_queries = [
    #
    # where equals
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_equals': {'primary_type': 'GROUND'},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(
            polars_pokemon['primary_type'] == 'GROUND'
        ),
    },
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_equals': {'name': 'Nidoran♀'},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(
            polars_pokemon['name'] == 'Nidoran♀'
        ),
    },
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_equals': {'height': 0.7},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(polars_pokemon['height'] == 0.7),
    },
    {
        'select_kwargs': {
            'table': 'simple',
            'output_format': 'polars',
            'where_equals': {'raw_data': b'\xde\xad\xbe\xef'},
        },
        'target_result': pl.DataFrame(
            simple['rows'], schema=simple_columns, orient='row'
        ).filter(
            pl.col('raw_data')
            == pl.Series([b'\xde\xad\xbe\xef'] * len(simple['rows']))
        ),
    },
    {
        'select_kwargs': {
            'table': simple_schema,
            'output_format': 'polars',
            'where_equals': {'raw_data': 'deadbeef'},
        },
        'target_result': pl.DataFrame(
            simple['rows'], schema=simple_columns, orient='row'
        ).filter(
            pl.col('raw_data')
            == pl.Series([b'\xde\xad\xbe\xef'] * len(simple['rows']))
        ),
    },
    {
        'select_kwargs': {
            'table': simple_schema,
            'output_format': 'polars',
            'where_equals': {'raw_data': '0xdeadbeef'},
        },
        'target_result': pl.DataFrame(
            simple['rows'], schema=simple_columns, orient='row'
        ).filter(
            pl.col('raw_data')
            == pl.Series([b'\xde\xad\xbe\xef'] * len(simple['rows']))
        ),
    },
    #
    # where lt
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_lt': {'height': 0.7},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(pl.col('height') < 0.7),
    },
    #
    # where lte
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_lte': {'height': 0.7},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(pl.col('height') <= 0.7),
    },
    #
    # where gt
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_gt': {'height': 0.7},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(pl.col('height') > 0.7),
    },
    #
    # where gte
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_gte': {'height': 0.7},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(pl.col('height') >= 0.7),
    },
    #
    # where like
    # {
    #     'select_kwargs': {
    #         'table': 'pokemon',
    #         'output_format': 'polars',
    #         'where_like': {'name': 'Char%'},
    #         'order_by': 'id',
    #     },
    #     'target_result': polars_pokemon.filter(
    #         pl.col('name').str.contains(r'Char')
    #     ),
    # },
    # {
    #     'select_kwargs': {
    #         'table': 'pokemon',
    #         'output_format': 'polars',
    #         'where_like': {'name': 'char%'},
    #         'order_by': 'id',
    #     },
    #     'target_result': polars_pokemon[0:0],
    # },
    #
    # where ilike
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_ilike': {'name': 'char%'},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(
            pl.col('name').str.contains(r'(?i)^char')
        ),
    },
    #
    # where in
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'where_in': {'primary_type': ['GROUND', 'ELECTRIC']},
            'order_by': 'id',
        },
        'target_result': polars_pokemon.filter(
            pl.col('primary_type').is_in(['GROUND', 'ELECTRIC'])
        ),
    },
    {
        'select_kwargs': {
            'table': 'simple',
            'output_format': 'polars',
            'where_in': {'raw_data': [b'\xde\xad\xbe\xef']},
        },
        'target_result': pl.DataFrame(
            simple['rows'], schema=simple_columns, orient='row'
        ).filter(
            pl.col('raw_data')
            == pl.Series([b'\xde\xad\xbe\xef'] * len(simple['rows']))
        ),
    },
    {
        'select_kwargs': {
            'table': simple_schema,
            'output_format': 'polars',
            'where_in': {'raw_data': ['deadbeef']},
        },
        'target_result': pl.DataFrame(
            simple['rows'], schema=simple_columns, orient='row'
        ).filter(
            pl.col('raw_data')
            == pl.Series([b'\xde\xad\xbe\xef'] * len(simple['rows']))
        ),
    },
    {
        'select_kwargs': {
            'table': simple_schema,
            'output_format': 'polars',
            'where_in': {'raw_data': ['0xdeadbeef']},
        },
        'target_result': pl.DataFrame(
            simple['rows'], schema=simple_columns, orient='row'
        ).filter(
            pl.col('raw_data')
            == pl.Series([b'\xde\xad\xbe\xef'] * len(simple['rows']))
        ),
    },
    #
    # where or
    {
        'select_kwargs': {
            'table': 'pokemon',
            'output_format': 'polars',
            'order_by': 'id',
            'where_or': [
                {'where_lte': {'total_stats': 200}},
                {'where_gte': {'total_stats': 500}},
            ],
        },
        'target_result': polars_pokemon.filter(
            (pl.col('total_stats') <= 200) | (pl.col('total_stats') >= 500)
        ),
    },
]


#
# # fixtures
#


@pytest.fixture(params=select_queries)
def select_query(request):
    return request.param


@pytest.fixture(params=parameterized_select_queries)
def parameterized_select_query(request):
    return request.param


#
# # basic select tests
#


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
    result = toolsql.select(conn=sync_read_bare_db_config, **select_kwargs)
    helpers.assert_results_equal(result=result, target_result=target_result)


#
# # async
#


async def test_async_select(async_read_conn_db_config, select_query, helpers):
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    async with toolsql.async_connect(async_read_conn_db_config) as conn:
        result = await toolsql.async_select(conn=conn, **select_kwargs)

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select_no_context(
    async_read_conn_db_config, select_query, helpers
):
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']
    conn = await (
        toolsql.async_connect(async_read_conn_db_config, as_context=False)
    )

    try:
        result = await toolsql.async_select(conn=conn, **select_kwargs)
    finally:
        await conn.close()

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select_bare_conn(
    async_read_bare_db_config, select_query, helpers
):
    select_kwargs = select_query['select_kwargs']
    target_result = select_query['target_result']

    result = await toolsql.async_select(
        conn=async_read_bare_db_config, **select_kwargs
    )

    helpers.assert_results_equal(result=result, target_result=target_result)


#
# # parameterized queries
#


def test_parameterized_sync_select(
    sync_dbapi_db_config, parameterized_select_query, helpers
):

    select_kwargs = parameterized_select_query['select_kwargs']
    target_result = parameterized_select_query['target_result']

    with toolsql.connect(sync_dbapi_db_config) as conn:
        result = toolsql.select(conn=conn, **select_kwargs)

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_parameterized_sync_select(
    async_dbapi_db_config, parameterized_select_query, helpers
):

    select_kwargs = parameterized_select_query['select_kwargs']
    target_result = parameterized_select_query['target_result']

    async with toolsql.async_connect(async_dbapi_db_config) as conn:
        result = await toolsql.async_select(conn=conn, **select_kwargs)

    helpers.assert_results_equal(result=result, target_result=target_result)

