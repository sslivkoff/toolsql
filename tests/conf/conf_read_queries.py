import conf.conf_tables as conf_tables

import pandas as pd
import polars as pl


test_tables = conf_tables.get_test_tables()
simple_table = test_tables['simple']
schema = simple_table['schema']
simple_columns = list(schema['columns'].keys())
_select_queries = {
    'SELECT * FROM simple': {
        'tuple': simple_table['rows'],
        'dict': [
            dict(zip(simple_columns, datum)) for datum in simple_table['rows']
        ],
        'polars': pl.DataFrame(simple_table['rows'], columns=simple_columns),
        'pandas': pd.DataFrame(simple_table['rows'], columns=simple_columns),
    },
}
select_queries = [
    {'sql': sql, 'output_format': output_format, 'target_result': target_result}
    for sql in _select_queries.keys()
    for output_format, target_result in _select_queries[sql].items()
]

