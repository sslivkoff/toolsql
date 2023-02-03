import polars as pl

import toolsql
import conf.conf_tables as conf_tables


test_tables = conf_tables.get_test_tables()

simple = test_tables['simple']
simple_columns = list(simple['schema']['columns'].keys())
polars_simple = pl.DataFrame(simple['rows'], columns=simple_columns)

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


def test_sync_insert_blank_table(
    sync_write_db_config, fresh_pokemon_table, helpers
):

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


def test_sync_insert_partial_table(
    sync_write_db_config, fresh_pokemon_table, helpers
):

    sync_db_config = sync_write_db_config
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows[:10], conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_pokemon[:10],
    )

    # insert rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows[10:20], conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_pokemon[:20],
    )

    # insert rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows[20:40], conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_pokemon,
    )


def test_sync_insert_on_conflict_ignore(
    sync_write_db_config, fresh_simple_table, helpers
):

    sync_db_config = sync_write_db_config
    schema = fresh_simple_table['schema']
    rows = fresh_simple_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows, conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_simple,
    )

    # insert conflicting rows, ignore them
    modified_rows = [row[:-1] + tuple(['MODIFIED']) for row in rows]
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(
            table=schema,
            rows=modified_rows,
            conn=conn,
            on_conflict='ignore',
        )

    # make sure ignored rows not present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema,
            order_by='id',
            conn=conn,
            output_format='polars',
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_simple,
    )


def test_sync_insert_on_conflict_update(
    sync_write_db_config, fresh_simple_table, helpers
):

    sync_db_config = sync_write_db_config
    schema = fresh_simple_table['schema']
    rows = fresh_simple_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(table=schema, rows=rows, conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_simple,
    )

    # insert conflicting rows, ignore them
    modified_rows = [row[:-1] + tuple(['MODIFIED']) for row in rows]
    with toolsql.connect(sync_db_config) as conn:
        toolsql.insert(
            table=schema,
            rows=modified_rows,
            conn=conn,
            on_conflict='update',
            columns=simple_columns,
        )

    # make sure ignored rows not present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema,
            order_by='id',
            conn=conn,
            output_format='polars',
        )
    helpers.assert_results_equal(
        result=result,
        target_result=pl.DataFrame(modified_rows, columns=simple_columns),
    )


#
# # async
#

async def test_async_insert_blank_table(
    async_write_db_config, fresh_pokemon_table, helpers
):

    async_db_config = async_write_db_config
    sync_db_config = toolsql.create_db_config(async_write_db_config, sync=True)
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(table=schema, rows=rows, conn=conn)

    # confirm table full
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(result=result, target_result=polars_pokemon)


async def test_async_insert_partial_table(
    async_write_db_config, fresh_pokemon_table, helpers
):

    async_db_config = async_write_db_config
    sync_db_config = toolsql.create_db_config(async_write_db_config, sync=True)
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(table=schema, rows=rows[:10], conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_pokemon[:10],
    )

    # insert rows
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(table=schema, rows=rows[10:20], conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_pokemon[:20],
    )

    # insert rows
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(table=schema, rows=rows[20:40], conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_pokemon,
    )


async def test_async_insert_on_conflict_ignore(
    async_write_db_config, fresh_simple_table, helpers
):

    async_db_config = async_write_db_config
    sync_db_config = toolsql.create_db_config(async_write_db_config, sync=True)
    schema = fresh_simple_table['schema']
    rows = fresh_simple_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(table=schema, rows=rows, conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_simple,
    )

    # insert conflicting rows, ignore them
    modified_rows = [row[:-1] + tuple(['MODIFIED']) for row in rows]
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(
            table=schema,
            rows=modified_rows,
            conn=conn,
            on_conflict='ignore',
        )

    # make sure ignored rows not present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema,
            order_by='id',
            conn=conn,
            output_format='polars',
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_simple,
    )


async def test_async_insert_on_conflict_update(
    async_write_db_config, fresh_simple_table, helpers
):

    async_db_config = async_write_db_config
    sync_db_config = toolsql.create_db_config(async_write_db_config, sync=True)
    schema = fresh_simple_table['schema']
    rows = fresh_simple_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(table=schema, rows=rows, conn=conn)

    # confirm table rows present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema, order_by='id', conn=conn, output_format='polars'
        )
    helpers.assert_results_equal(
        result=result,
        target_result=polars_simple,
    )

    # insert conflicting rows, ignore them
    modified_rows = [row[:-1] + tuple(['MODIFIED']) for row in rows]
    async with toolsql.async_connect(async_db_config) as conn:
        await toolsql.async_insert(
            table=schema,
            rows=modified_rows,
            conn=conn,
            on_conflict='update',
            columns=simple_columns,
        )

    # make sure ignored rows not present
    with toolsql.connect(sync_db_config) as conn:
        result = toolsql.select(
            table=schema,
            order_by='id',
            conn=conn,
            output_format='polars',
        )
    helpers.assert_results_equal(
        result=result,
        target_result=pl.DataFrame(modified_rows, columns=simple_columns),
    )

