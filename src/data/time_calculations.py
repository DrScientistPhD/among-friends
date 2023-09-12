import warnings

import numpy as np
import pandas as pd

from src.data.data_validation import (validate_columns_in_dataframe,
                                      validate_data_types, validate_dataframe)


class TimeCalculations:
    @staticmethod
    def calculate_time_diff(df: pd.DataFrame, timestamp1: str, timestamp2: str) -> pd.DataFrame:
        """
        Calculates the time difference between two timestamps (Unix time in milliseconds) and stores the result in a
        new column 'time_diff'. The time difference is given in seconds. A warning is raised if any time difference is
        negative.

        Args:
            df (pd.DataFrame): The DataFrame containing the time columns.
            timestamp1 (str): The name of the first time column.
            timestamp2 (str): The name of the second time column.

        Returns:
            pd.DataFrame: The original DataFrame with an added 'time_diff' column.

        Raises:
            Exception: If there's an error during the time difference calculation.
            Warning: If there is a negative time difference
        """
        # TODO: Need to figure out why some time_diff values are negative

        validate_dataframe(df)
        validate_data_types(timestamp1, str, "timestamp1")
        validate_data_types(timestamp2, str, "timestamp2")
        validate_columns_in_dataframe(df, [timestamp1, timestamp2])

        try:
            df["time_diff"] = df.apply(
                lambda row: 0
                if row[timestamp1] == row[timestamp2]
                else (row[timestamp1] - row[timestamp2]) / 1000,
                axis=1,
            )
        except Exception as e:
            raise Exception(f"Failed to calculate time difference: {str(e)}")

        if (df["time_diff"] < 0).any():
            warnings.warn(
                "There are negative time differences in the data", UserWarning
            )

        return df

    @staticmethod
    def calculate_half_life(df: pd.DataFrame, column: str, percentile: float = 0.75) -> float:
        """
        Calculates the half-life based on a specified percentile of a specified column in the given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with at least one numerical column.
            column (str): Name of the column based on which the decay constant is to be calculated.
            percentile (float, optional): Percentile value to be used in the decay constant calculation.
                Must be between 0 and 1 (exclusive). Defaults to 0.75.

        Returns:
            float: Half life calculated as log(2) divided by the specified percentile of the specified column.

        Raises:
            ValueError: If percentile is not between 0 and 1.
            Exception: If there's an error during the half life calculation.
        """
        validate_dataframe(df)
        validate_data_types(column, str, "column")
        validate_columns_in_dataframe(df, [column])
        validate_data_types(percentile, (float, int), "percentile")

        # Additional validation to ensure percentile is between 0 and 1
        if not 0 <= percentile <= 1:
            raise ValueError("percentile must be between 0 and 1")

        try:
            # TODO: I screwed up the naming here. decay_constant (t1/2) and half_life (lambda) should be reversed.
            decay_constant = df[column].quantile(percentile)
            half_life = np.log(2) / decay_constant
            return half_life

        except Exception as e:
            raise Exception(f"Failed to calculate half life: {str(e)}")

    @staticmethod
    def calculate_weight(
        df: pd.DataFrame, half_life: float, base_value: float
    ) -> pd.DataFrame:
        """
        Calculates the weight using the provided half life.

        Args:
            df (pd.DataFrame): DataFrame with at least a numerical column 'time_diff'.
            half_life (float): Half life for the calculation.
            base_value (float): Base value for the weight calculation.

        Returns:
            pd.DataFrame: Original DataFrame with an additional 'weight' column.

        Raises:
            TypeError: If df is not a pandas DataFrame, or if half_life or base_value are not floats.
            KeyError: If the time_diff column is not in the provided DataFrame.
            Exception: If there's an error during the weight calculation.
        """
        # Validate input data
        validate_dataframe(df)
        validate_data_types(half_life, float, "half_life")
        validate_data_types(base_value, float, "base_value")
        validate_columns_in_dataframe(df, ["time_diff"])

        try:
            df["weight"] = base_value * np.exp(-half_life * df["time_diff"])
            return df

        except Exception as e:
            raise Exception(f"Failed to calculate weight: {str(e)}")
