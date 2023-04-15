import numpy as np
import pandas as pd
import polars as pl


class ToolsqlTestHelpers:
    """adapting helper pattern from https://stackoverflow.com/a/42156088"""

    @staticmethod
    def assert_results_equal(result, target_result, check_names=True):
        if isinstance(target_result, pl.DataFrame):
            try:
                assert target_result.frame_equal(result)
            except pl.PolarsPanicError:
                assert target_result.dtypes == result.dtypes
                assert target_result.columns == result.columns
                for column in target_result.columns:
                    assert list(target_result[column]) == list(result[column])
        elif isinstance(target_result, pd.DataFrame):
            assert np.all(target_result.columns.values == result.columns.values)
            for name, column in target_result.items():
                assert np.all(column.values == result[name].values)
        elif isinstance(target_result, dict):
            assert type(result) is dict
            actual_keys = set(result.keys())
            target_keys = set(target_result.keys())
            assert actual_keys == target_keys
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
        elif isinstance(target_result, pl.Series):
            assert type(result) == type(target_result)
            assert len(result) == len(target_result)
            if check_names:
                assert result.series_equal(target_result)
            else:
                np.all(result.to_numpy() == target_result.to_numpy())
        else:
            assert type(result) == type(target_result)
            assert result == target_result

