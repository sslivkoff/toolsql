import polars as pl

import toolsql
import conf.conf_tables as conf_tables


test_tables = conf_tables.get_test_tables()
pokemon = test_tables['pokemon']
pokemon_columns = list(pokemon['schema']['columns'].keys())
polars_pokemon = pl.DataFrame(pokemon['rows'], schema=pokemon_columns)
polars_pokemon = polars_pokemon.with_columns(
    pl.Series(
        values=[list(item) for item in polars_pokemon['all_types']],
        dtype=pl.Object,
        name='all_types',
    )
)
pokemon = polars_pokemon


def test_renaming_results(sync_read_conn_db_config, helpers):
    with toolsql.connect(sync_read_conn_db_config) as conn:
        result = toolsql.select(
            columns={'name': 'full_name', 'hp': 'health'},
            table='pokemon',
            output_format='polars',
            conn=conn,
        )
    assert set(result.columns) == {'full_name', 'health'}
    helpers.assert_results_equal(
        result['full_name'], pokemon['name'], check_names=False
    )
    helpers.assert_results_equal(
        result['health'], pokemon['hp'], check_names=False
    )

