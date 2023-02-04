# from __future__ import annotations

# from toolsql import spec

# """

# 1.
#     with toolsql.connect(db_config) as conn:
#         with toolsql.begin(conn):
#             ...
#             toolsql.rollback(conn)

# 2.
#     conn = toolsql.connect(db_config)
#     with toolsql.begin(conn):
#         ...
#         toolsql.rollback(conn)

# 3.
#     conn = toolsql.connect(db_config)
#     toolsql.begin(conn)
#     ...
#     toolsql.commit(conn) | toolsql.rollback(conn)

# 4.
#     with toolsql.connect(db_config) as conn:
#         toolsql.begin(conn)
#         ...
#         toolsql.commit(conn) | toolsql.rollback(conn)


# """


# def begin(
#     conn: spec.Connection,
#     uri: str | None = None,
#     db_config: spec.DBConfig | None = None,
# ) -> spec.Transaction:
#     driver = drivers.get_driver(uri=uri, db_config=db_config, sync=True)
#     return driver.begin(uri=uri)


# def begin_async(
#     uri: str | None = None,
#     db_config: spec.DBConfig | None = None,
# ) -> spec.AsyncTransaction:

#     if uri is None:
#         if db_config is None:
#             raise Exception('must specify uri or db_config')
#         uri = get_db_uri(db_config=db_config)
#     driver = drivers.get_driver(uri=uri, db_config=db_config, sync=True)
#     return driver.begin_async(uri=uri)

