import polars as pl
import pytest

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


class CustomInnerException(Exception):
    pass


def test_sync_tx_fail(sync_write_db_config, fresh_pokemon_table, helpers):

    db_config = sync_write_db_config
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows in failed transaction
    with pytest.raises(CustomInnerException):
        with toolsql.connect(db_config) as conn:
            with toolsql.transaction(conn):
                toolsql.insert(table=schema, rows=rows[:5], conn=conn)
                raise CustomInnerException()

    # confirm table full
    with toolsql.connect(db_config) as conn:
        result = toolsql.select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 0

    # insert rows in non-failed transaction
    with pytest.raises(CustomInnerException):
        with toolsql.connect(db_config) as conn:
            with toolsql.transaction(conn):
                toolsql.insert(table=schema, rows=rows[:5], conn=conn)
            raise CustomInnerException()

    # confirm table full
    with toolsql.connect(db_config) as conn:
        result = toolsql.select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 5

    # insert rows in failed non-context transaction
    with pytest.raises(CustomInnerException):
        with toolsql.connect(db_config) as conn:
            toolsql.begin(conn)
            toolsql.insert(table=schema, rows=rows[5:10], conn=conn)
            raise CustomInnerException()

    # confirm table full
    with toolsql.connect(db_config) as conn:
        result = toolsql.select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 5

    # insert rows in non-context transaction
    with pytest.raises(CustomInnerException):
        with toolsql.connect(db_config) as conn:
            toolsql.begin(conn)
            toolsql.insert(table=schema, rows=rows[5:10], conn=conn)
            toolsql.commit(conn)
            raise CustomInnerException()

    # confirm table full
    with toolsql.connect(db_config) as conn:
        result = toolsql.select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 10


async def test_async_tx_fail(
    async_write_db_config, fresh_pokemon_table, helpers
):

    db_config = async_write_db_config
    sync_db_config = db_config.copy()
    del sync_db_config['driver']
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows in failed transaction
    with pytest.raises(CustomInnerException):
        async with toolsql.async_connect(db_config) as conn:
            async with toolsql.async_transaction(conn):
                await toolsql.async_insert(
                    table=schema, rows=rows[:5], conn=conn
                )
                raise CustomInnerException()

    # confirm table full
    async with toolsql.async_connect(db_config) as conn:
        result = await toolsql.async_select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 0

    # insert rows alongside Exception
    async with toolsql.async_connect(db_config) as conn:
        async with toolsql.async_transaction(conn):
            await toolsql.async_insert(table=schema, rows=rows[:5], conn=conn)

    # confirm table full
    async with toolsql.async_connect(db_config) as conn:
        result = await toolsql.async_select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 5

    # insert rows in failed non-context transaction
    with pytest.raises(CustomInnerException):
        async with toolsql.async_connect(db_config) as conn:
            await toolsql.async_begin(conn)
            await toolsql.async_insert(table=schema, rows=rows[5:10], conn=conn)
            raise CustomInnerException()

    # confirm table full
    async with toolsql.async_connect(db_config) as conn:
        result = await toolsql.async_select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 5

    # insert rows in non-context transaction
    with pytest.raises(CustomInnerException):
        async with toolsql.async_connect(db_config) as conn:
            await toolsql.async_begin(conn)
            await toolsql.async_insert(table=schema, rows=rows[5:10], conn=conn)
            await toolsql.async_commit(conn)
            raise CustomInnerException()

    # confirm table full
    async with toolsql.async_connect(db_config) as conn:
        result = await toolsql.async_select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 10


def test_autocommit(sync_write_db_config, fresh_pokemon_table, helpers):

    db_config = sync_write_db_config
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows without autocommit, so they fail
    with pytest.raises(CustomInnerException):
        with toolsql.connect(db_config, autocommit=False) as conn:
            toolsql.insert(table=schema, rows=rows[:5], conn=conn)
            raise CustomInnerException()

    # confirm table full
    with toolsql.connect(db_config) as conn:
        result = toolsql.select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 0

    # insert rows with autocommit, so they succeed
    with pytest.raises(CustomInnerException):
        with toolsql.connect(db_config, autocommit=True) as conn:
            toolsql.insert(table=schema, rows=rows[:5], conn=conn)
            raise CustomInnerException()

    # confirm table full
    with toolsql.connect(db_config) as conn:
        result = toolsql.select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 5


async def test_autocommit_async(
    async_write_db_config, fresh_pokemon_table, helpers
):

    db_config = async_write_db_config
    sync_db_config = db_config.copy()
    del sync_db_config['driver']
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # create table
    with toolsql.connect(sync_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    # insert rows without autocommit, so they fail
    with pytest.raises(CustomInnerException):
        async with toolsql.async_connect(db_config, autocommit=False) as conn:
            await toolsql.async_insert(table=schema, rows=rows[:5], conn=conn)
            raise CustomInnerException()

    # confirm table full
    async with toolsql.async_connect(db_config) as conn:
        result = await toolsql.async_select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 0

    # insert rows with autocommit, so they succeed
    with pytest.raises(CustomInnerException):
        async with toolsql.async_connect(db_config, autocommit=True) as conn:
            await toolsql.async_insert(table=schema, rows=rows[:5], conn=conn)
            raise CustomInnerException()

    # confirm table full
    async with toolsql.async_connect(db_config) as conn:
        result = await toolsql.async_select(
            table=schema,
            conn=conn,
            columns=['COUNT(*)'],
            output_format='cell',
        )
        assert result == 5

