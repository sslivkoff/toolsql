import toolsql
import conf.conf_tables as conf_tables


def test_dbms_table_names(sync_dbapi_db_config):
    with toolsql.connect(sync_dbapi_db_config) as conn:
        table_names = toolsql.get_table_names(conn)
    assert set(table_names) == {'pokemon', 'simple'}


def test_dbms_table_schema(sync_dbapi_db_config, helpers):

    with toolsql.connect(sync_dbapi_db_config) as conn:
        actual_pokemon_schema = toolsql.get_table_schema('pokemon', conn=conn)
    pokemon_table = conf_tables.get_pokemon_table()
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


