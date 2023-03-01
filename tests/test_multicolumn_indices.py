import pytest

import conf.conf_tables
import conf.conf_helpers

import toolsql


def test_composite_primary_table(sync_write_db_config):

    table = conf.conf_tables.get_weather_table()

    schema = table['schema']
    schema = toolsql.normalize_shorthand_table_schema(schema)
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    rows = table['rows']
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.insert(rows=rows, table=schema, conn=conn)


def test_multi_column_unique_constraint(sync_write_db_config):

    table = conf.conf_tables.get_history_table(random_name=True)

    schema = table['schema']
    schema = toolsql.normalize_shorthand_table_schema(schema)
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    rows = table['rows']
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.insert(rows=rows, table=schema, conn=conn)

    for row in table['conflict_rows']:
        with toolsql.connect(sync_write_db_config) as conn:
            with pytest.raises(Exception):
                toolsql.insert(row=row, conn=conn, table=schema)

