import toolsql


# def test_dbms_table_names(sync_read_db_config, db_schema):
#     conn = toolsql.connect(sync_read_db_config)
#     table_names = toolsql.get_table_names(conn)
#     assert set(table_names) == set(db_schema['tables'].keys())


# def test_dbms_table_schema(sync_read_db_config, db_schema, helpers):
#     conn = toolsql.connect(sync_read_db_config)

#     for target_table_schema in db_schema['tables']:
#         actual_table_schema = toolsql.get_table_metadata(
#             table_name=target_table_schema['name'], conn=conn
#         )
#         assert helpers.assert_results_equal(
#             actual_table_schema, target_table_schema
#         )


# def test_dbms_get_create_statement():
#     raise NotImplementedError()

