import pytest
import toolsql


def test_select_table_dne(sync_read_conn_db_config, fresh_pokemon_table):

    with pytest.raises(toolsql.TableDoesNotExist):
        with toolsql.connect(sync_read_conn_db_config) as conn:
            toolsql.select(conn=conn, columns=['COUNT(*)'], table='hi')


async def test_select_table_dne_async(
    async_read_conn_db_config, fresh_pokemon_table
):

    with pytest.raises(toolsql.TableDoesNotExist):
        async with toolsql.async_connect(async_read_conn_db_config) as conn:
            await toolsql.async_select(
                conn=conn, columns=['COUNT(*)'], table='hi'
            )


def test_table_dne(sync_write_db_config, fresh_pokemon_table, helpers):

    sync_db_config = sync_write_db_config
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # insert rows
    with pytest.raises(toolsql.TableDoesNotExist):
        with toolsql.connect(sync_db_config) as conn:
            toolsql.insert(table=schema, rows=rows, conn=conn)

    # update rows
    with pytest.raises(toolsql.TableDoesNotExist):
        with toolsql.connect(sync_db_config) as conn:
            toolsql.update(
                table=schema,
                where_equals={},
                values={'a': 1},
                columns=['a'],
                conn=conn,
            )

    # delete rows
    with pytest.raises(toolsql.TableDoesNotExist):
        with toolsql.connect(sync_db_config) as conn:
            toolsql.delete(table=schema, where_equals={}, conn=conn)


async def test_table_dne_async(
    async_write_db_config, fresh_pokemon_table, helpers
):

    async_db_config = async_write_db_config
    schema = fresh_pokemon_table['schema']
    rows = fresh_pokemon_table['rows']

    # insert rows
    with pytest.raises(toolsql.TableDoesNotExist):
        async with toolsql.async_connect(async_db_config) as conn:
            await toolsql.async_insert(table=schema, rows=rows, conn=conn)

    # update rows
    with pytest.raises(toolsql.TableDoesNotExist):
        async with toolsql.async_connect(async_db_config) as conn:
            await toolsql.async_update(
                table=schema,
                where_equals={},
                values={'a': 1},
                columns=['a'],
                conn=conn,
            )

    # delete rows
    with pytest.raises(toolsql.TableDoesNotExist):
        async with toolsql.async_connect(async_db_config) as conn:
            await toolsql.async_delete(table=schema, where_equals={}, conn=conn)

