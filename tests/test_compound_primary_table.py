
import conf.conf_tables

import toolsql


def test_compound_primary_table(sync_write_db_config):

    table = conf.conf_tables.get_weather_table()

    schema = table['schema']
    schema = toolsql.normalize_shorthand_table_schema(schema)
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    rows = table['rows']
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.insert(rows=rows, table=schema, conn=conn)

