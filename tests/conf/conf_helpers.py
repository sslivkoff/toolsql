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
        elif isinstance(target_result, dict):
            assert type(result) is dict
            assert set(result.keys()) == set(target_result.keys())
            for key, value in result.items():
                ToolsqlTestHelpers.assert_results_equal(
                    value, target_result[key]
                )
        elif isinstance(target_result, (list, tuple)):
            assert type(result) == type(target_result)
            assert len(result) == len(target_result)
            for value, target_value in zip(result, target_result):
                ToolsqlTestHelpers.assert_results_equal(
                    value, target_value
                )
        else:
            assert result == target_result

