import toolsql


def test_row_count(sync_read_conn_db_config):
    with toolsql.connect(sync_read_conn_db_config) as conn:
        row_count = toolsql.get_table_row_count(
            table='pokemon',
            conn=conn,
        )
    assert row_count == 40


def test_table_nbytes(sync_read_conn_db_config):
    with toolsql.connect(sync_read_conn_db_config) as conn:
        nbytes = toolsql.get_table_nbytes(
            table='pokemon',
            conn=conn,
        )
    assert nbytes > 0


def test_tables_nbytes(sync_read_conn_db_config):
    with toolsql.connect(sync_read_conn_db_config) as conn:
        nbytes = toolsql.get_tables_nbytes(
            conn=conn,
        )
    assert 'pokemon' in nbytes
    assert 'simple' in nbytes


async def test_async_row_count(async_read_conn_db_config):
    async with toolsql.async_connect(async_read_conn_db_config) as conn:
        row_count = await toolsql.async_get_table_row_count(
            table='pokemon',
            conn=conn,
        )
    assert row_count == 40


async def test_async_table_nbytes(async_read_conn_db_config):
    async with toolsql.async_connect(async_read_conn_db_config) as conn:
        nbytes = await toolsql.async_get_table_nbytes(
            table='pokemon',
            conn=conn,
        )
    assert nbytes > 0


async def test_async_tables_nbytes(async_read_conn_db_config):
    async with toolsql.async_connect(async_read_conn_db_config) as conn:
        nbytes = await toolsql.async_get_tables_nbytes(
            conn=conn,
        )
    assert 'pokemon' in nbytes
    assert 'simple' in nbytes
