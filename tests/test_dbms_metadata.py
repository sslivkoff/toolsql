import pytest
import toolsql

import conf.conf_tables
import conf.conf_helpers


def test_dbms_table_names(sync_dbapi_db_config):
    with toolsql.connect(sync_dbapi_db_config) as conn:
        table_names = toolsql.get_table_names(conn)
    assert {'pokemon', 'simple'}.issubset(set(table_names))


def test_dbms_table_schema(sync_dbapi_db_config, helpers):

    with toolsql.connect(sync_dbapi_db_config) as conn:
        actual_pokemon_schema = toolsql.get_table_schema('pokemon', conn=conn)
    pokemon_table = conf.conf_tables.get_pokemon_table()
    pokemon_schema = toolsql.normalize_shorthand_table_schema(
        pokemon_table['schema']
    )
    pokemon_schema = toolsql.convert_table_schema_to_dialect(
        pokemon_schema, dialect=sync_dbapi_db_config['dbms']
    )
    helpers.assert_results_equal(actual_pokemon_schema, pokemon_schema)


def test_has_table(sync_dbapi_db_config, helpers):
    with toolsql.connect(sync_dbapi_db_config) as conn:
        has_table = toolsql.has_table('pokemon', conn=conn)
        assert has_table

        has_table = toolsql.has_table('pokemon_nope', conn=conn)
        assert not has_table


@pytest.mark.parametrize(
    'table',
    [
        conf.conf_tables.get_simple_table(random_name=True),
        conf.conf_tables.get_pokemon_table(random_name=True),
        conf.conf_tables.get_weather_table(random_name=True),
        conf.conf_tables.get_history_table(random_name=True),
    ],
)
def test_db_schema_reader(table, sync_write_db_config):

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

