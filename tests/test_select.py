import toolsql


def test_sync_select(sync_read_conn_db_config, select_query, helpers):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']

    with toolsql.connect(sync_read_conn_db_config) as conn:
        result = toolsql.select(sql=sql, conn=conn, output_format=output_format)

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_no_context(
    sync_read_conn_db_config, select_query, helpers
):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    conn = toolsql.connect(sync_read_conn_db_config, as_context=False)

    try:
        result = toolsql.select(sql=sql, conn=conn, output_format=output_format)
    finally:
        conn.close()

    helpers.assert_results_equal(result=result, target_result=target_result)


def test_sync_driver_bare_conn(sync_read_bare_db_config, select_query, helpers):

    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    result = toolsql.select(
        sql=sql, conn=sync_read_bare_db_config, output_format=output_format
    )
    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select(async_read_conn_db_config, select_query, helpers):
    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    async with toolsql.async_connect(async_read_conn_db_config) as conn:
        result = await toolsql.async_select(
            sql=sql, conn=conn, output_format=output_format
        )

    helpers.assert_results_equal(result=result, target_result=target_result)


async def test_async_select_no_context(
    async_read_conn_db_config, select_query, helpers
):
    sql = select_query['sql']
    output_format = select_query['output_format']
    target_result = select_query['target_result']
    conn = await toolsql.async_connect(
        async_read_conn_db_config, as_context=False
    )

    try:
        result = await toolsql.async_select(
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

    result = await toolsql.async_select(
        sql=sql, conn=async_read_bare_db_config, output_format=output_format
    )

    helpers.assert_results_equal(result=result, target_result=target_result)

