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


def test_db_schema_reader(sync_write_db_config):

    table = conf.conf_tables.get_history_table(random_name=True)

    schema = table['schema']
    schema = toolsql.normalize_shorthand_table_schema(
        schema,
        dialect=sync_write_db_config['dbms'],
    )
    with toolsql.connect(sync_write_db_config) as conn:
        toolsql.create_table(table=schema, conn=conn, confirm=True)

    with toolsql.connect(sync_write_db_config) as conn:
        actual_schema = toolsql.get_table_schema(
            table=schema['name'],
            conn=conn,
        )

    conf.conf_helpers.ToolsqlTestHelpers.assert_results_equal(
        result=actual_schema,
        target_result=schema,
    )


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

