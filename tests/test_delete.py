# import toolsql


# def test_sync_delete(sync_write_db_config, delete_query, helpers):
#     sql = delete_query['sql']
#     parameters = delete_query['parameters']

#     with toolsql.connect(sync_write_db_config) as conn:
#         result = toolsql.delete(
#             sql=sql,
#             parameters=parameters,
#             conn=conn,
#         )
#     if 'target_result' in delete_query:
#         target_result = delete_query['target_result']
#         helpers.assert_results_equal(result, target_result)

#     with toolsql.connect(sync_write_db_config) as conn:
#         for assertion in delete_query.get('assertions'):
#             select_result = toolsql.select(
#                 sql=assertion['select_sql'],
#                 parameters=assertion['select_paramters'],
#                 conn=conn,
#             )
#             helpers.assert_results_equal(
#                 select_result,
#                 assertion['target_result'],
#             )


# async def test_async_delete(async_write_db_config, delete_query):
#     pass

