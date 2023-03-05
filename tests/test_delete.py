import conf.conf_tables as conf_tables

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

polars_pokemon = pl.DataFrame(pokemon['rows'], schema=pokemon_columns)
polars_pokemon = polars_pokemon.with_columns(
    pl.Series(
        values=[list(item) for item in polars_pokemon['all_types']],
        dtype=pl.Object,
        name='all_types',
    )
)

delete_queries = [
    #
    # where equals
    {
        'delete_kwargs': {'where_equals': {'primary_type': 'GROUND'}},
        'target_result': polars_pokemon.filter(
            polars_pokemon['primary_type'] != 'GROUND'
        ),
    },
    {
        'delete_kwargs': {'where_equals': {'name': 'Nidoran♀'}},
        'target_result': polars_pokemon.filter(
            polars_pokemon['name'] != 'Nidoran♀'
        ),
    },
    {
        'delete_kwargs': {'where_equals': {'height': 0.7}},
        'target_result': polars_pokemon.filter(polars_pokemon['height'] != 0.7),
    },
    #
    # where lt
    {
        'delete_kwargs': {'where_lt': {'height': 0.7}},
        'target_result': polars_pokemon.filter(pl.col('height') >= 0.7),
    },
    #
    # where lte
    {
        'delete_kwargs': {'where_lte': {'height': 0.7}},
        'target_result': polars_pokemon.filter(pl.col('height') > 0.7),
    },
    #
    # where gt
    {
        'delete_kwargs': {'where_gt': {'height': 0.7}},
        'target_result': polars_pokemon.filter(pl.col('height') <= 0.7),
    },
    #
    # where gte
    {
        'delete_kwargs': {'where_gte': {'height': 0.7}},
        'target_result': polars_pokemon.filter(pl.col('height') < 0.7),
    },
    #
    # where ilike
    {
        'delete_kwargs': {'where_ilike': {'name': 'char%'}},
        'target_result': polars_pokemon.filter(
            ~pl.col('name').str.contains(r'(?i)^char')
        ),
    },
    #
    # where in
    {
        'delete_kwargs': {'where_in': {'primary_type': ['GROUND', 'ELECTRIC']}},
        'target_result': polars_pokemon.filter(
            ~pl.col('primary_type').is_in(['GROUND', 'ELECTRIC'])
        ),
    },
]


#
# # fixtures
#


@pytest.fixture(params=delete_queries)
def delete_query(request):
    return request.param


#
# # basic delete tests
#


def test_sync_delete(
    sync_write_db_config, delete_query, fresh_pokemon_table, helpers
):

    #
    # # setup table
    #

    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.insert(table=schema, rows=rows, conn=conn)

    # confirm table full
    with toolsql.connect(sync_write_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(result=result, target_result=polars_pokemon)

    #
    # # perform delete query
    #

    delete_kwargs = delete_query['delete_kwargs']
    target_result = delete_query['target_result']
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.delete(table=schema, conn=conn, **delete_kwargs)
        result = toolsql.select(table=schema, conn=conn, output_format='polars')
    helpers.assert_results_equal(result=result, target_result=target_result)

    #
    # # cleanup
    #

    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.drop_table(table=schema, conn=conn, confirm=True)


async def test_async_delete(
    async_write_db_config, delete_query, fresh_pokemon_table, helpers
):

    #
    # # setup table
    #

    sync_db_config = toolsql.create_db_config(async_write_db_config, sync=True)
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows, conn=conn)

    # confirm table full
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(result=result, target_result=polars_pokemon)

    #
    # # perform delete query
    #

    delete_kwargs = delete_query['delete_kwargs']
    target_result = delete_query['target_result']
    async with toolsql.async_connect(async_write_db_config) as conn:
        await toolsql.async_delete(table=schema, conn=conn, **delete_kwargs)
        result = await toolsql.async_select(
            table=schema, conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(result=result, target_result=target_result)

    #
    # # cleanup
    #

    with toolsql.connect(sync_db_config) as conn:
        toolsql.drop_table(table=schema, conn=conn, confirm=True)

