import polars as pl

import toolsql
import conf.conf_tables as conf_tables


test_tables = conf_tables.get_test_tables()


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


def test_sync_update(sync_write_db_config, fresh_pokemon_table, helpers):

    sync_db_config = sync_write_db_config
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows, conn=conn)

    # confirm table full
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(result=result, target_result=polars_pokemon)

    # update rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.update(
            conn=conn,
            table=schema,
            columns=['hp'],
            values=[999],
            where_equals={'primary_type': 'GROUND'},
        )

    # check result
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    target_result = polars_pokemon.with_columns(
        pl.when(pl.col('primary_type') == 'GROUND')
        .then(999)
        .otherwise(pl.col('hp'))
        .alias('hp')
    )
    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_update(async_write_db_config, fresh_pokemon_table, helpers):

    async_db_config = async_write_db_config
    sync_db_config = toolsql.create_db_config(async_write_db_config, sync=True)
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows, conn=conn)

    # confirm table full
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(result=result, target_result=polars_pokemon)

    # update rows
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_update(
            conn=conn,
            table=schema,
            columns=['hp'],
            values=[999],
            where_equals={'primary_type': 'GROUND'},
        )

    # check result
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    target_result = polars_pokemon.with_columns(
        pl.when(pl.col('primary_type') == 'GROUND')
        .then(999)
        .otherwise(pl.col('hp'))
        .alias('hp')
    )
    helpers.assert_results_equal(result=result, target_result=target_result)

