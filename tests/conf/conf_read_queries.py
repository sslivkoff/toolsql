import conf.conf_tables as conf_tables

import pandas as pd
import polars as pl


simple_table = conf_tables.simple_table
simple_columns = simple_table['columns'].keys()
_select_queries = {
    'SELECT * FROM simple_table': {
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

