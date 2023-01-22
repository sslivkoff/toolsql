
import numpy as np
import pandas as pd
import polars as pl


class ToolsqlTestHelpers:
    """adapting helper pattern from https://stackoverflow.com/a/42156088"""
    @staticmethod
    def assert_results_equal(result, target_result):
        if isinstance(target_result, pl.DataFrame):
            assert target_result.frame_equal(result)
        elif isinstance(target_result, pd.DataFrame):
            assert np.all(target_result.columns.values == result.columns.values)
            for name, column in target_result.items():
                assert np.all(column.values == result[name].values)
        else:
            assert result == target_result

