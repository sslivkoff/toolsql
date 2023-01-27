# def begin(
#     uri: str | None = None,
#     db_config: spec.DBConfig | None = None,
# ) -> spec.Transaction:

#     if uri is None:
#         if db_config is None:
#             raise Exception('must specify uri or db_config')
#         uri = get_db_uri(db_config=db_config)
#     driver = driver_utils.get_driver(uri=uri, db_config=db_config, sync=True)
#     return driver.begin(uri=uri)


# def begin_async(
#     uri: str | None = None,
#     db_config: spec.DBConfig | None = None,
# ) -> spec.AsyncTransaction:

#     if uri is None:
#         if db_config is None:
#             raise Exception('must specify uri or db_config')
#         uri = get_db_uri(db_config=db_config)
#     driver = driver_utils.get_driver(uri=uri, db_config=db_config, sync=True)
#     return driver.begin_async(uri=uri)

