import math

import pytest
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
pokemon_schema = toolsql.normalize_shorthand_table_schema(pokemon['schema'])
pokemon = polars_pokemon

aggregation_queries = [
    {
        'select_kwargs': {
            'columns': ['COUNT(*)'],
            'table': 'pokemon',
            'output_format': 'cell',
        },
        'target_result': len(pokemon['total_stats']),
    },
    {
        'select_kwargs': {
            'columns': ['COUNT(DISTINCT primary_type)'],
            'table': 'pokemon',
            'output_format': 'cell',
        },
        'target_result': len(set(pokemon['primary_type'])),
    },
    {
        'select_kwargs': {
            'columns': ['MIN(total_stats)'],
            'table': 'pokemon',
            'output_format': 'cell',
        },
        'target_result': pokemon['total_stats'].min(),
    },
    {
        'select_kwargs': {
            'columns': ['MAX(total_stats)'],
            'table': 'pokemon',
            'output_format': 'cell',
        },
        'target_result': pokemon['total_stats'].max(),
    },
    {
        'select_kwargs': {
            'columns': ['SUM(total_stats)'],
            'table': pokemon_schema,
            'output_format': 'cell',
        },
        'target_result': pokemon['total_stats'].sum(),
    },
    {
        'select_kwargs': {
            'columns': ['AVG(total_stats)'],
            'table': 'pokemon',
            'output_format': 'cell',
        },
        'target_result': pokemon['total_stats'].mean(),
    },
]


@pytest.mark.parametrize('query', aggregation_queries)
def test_aggregation_query(query, sync_read_conn_db_config, helpers):
    with toolsql.connect(sync_read_conn_db_config) as conn:
        actual_result = toolsql.select(conn=conn, **query['select_kwargs'])

    if isinstance(query['target_result'], float):
        assert math.isclose(actual_result, query['target_result'])
    else:
        helpers.assert_results_equal(actual_result, query['target_result'])

