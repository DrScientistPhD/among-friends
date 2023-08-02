import numpy as np
import pandas as pd
from src.data.data_validation import (validate_columns_in_dataframe,
                             validate_data_types, validate_dataframe)


class TimeCalculations:
    @staticmethod
    def calculate_time_diff(df: pd.DataFrame, time1: str, time2: str) -> pd.DataFrame:
        """
        Calculates the time difference between two time columns of a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame containing the time columns.
            time1 (str): The name of the first time column.
            time2 (str): The name of the second time column.

        Returns:
            pd.DataFrame: The original DataFrame with an added 'time_diff' column.

        Raises:
            Exception: If there's an error during the time difference calculation.
        """
        try:
            validate_dataframe(df)
            validate_columns_in_dataframe(df, [time1, time2])

            df["time_diff"] = (df[time1] - df[time2]).dt.total_seconds()
            return df

        except Exception as e:
            raise Exception(f"Failed to calculate time difference: {str(e)}")

    @staticmethod
    def calculate_decay_constant(half_life: float) -> float:
        """
        Calculates the decay constant given a half-life.

        Args:
            half_life (float): The half-life used to calculate the decay constant.

        Returns:
            float: The calculated decay constant.

        Raises:
            Exception: If there's an error during the decay constant calculation.
        """
        try:
            validate_data_types(half_life, (int, float), "half_life")

            decay_constant = -np.log(2) / half_life
            return decay_constant

        except Exception as e:
            raise Exception(f"Failed to calculate decay constant: {str(e)}")

    @staticmethod
    def calculate_weight(
        df: pd.DataFrame, decay_constant: float, time_diff: str
    ) -> pd.DataFrame:
        """
        Calculates the weight of each row in a DataFrame based on a decay constant and time difference.

        Args:
            df (pd.DataFrame): The DataFrame containing the time difference column.
            decay_constant (float): The decay constant used to calculate the weight.
            time_diff (str): The name of the time difference column.

        Returns:
            pd.DataFrame: The original DataFrame with an added 'weight' column.

        Raises:
            Exception: If there's an error during the weight calculation.
        """
        try:
            validate_dataframe(df)
            validate_columns_in_dataframe(df, [time_diff])
            validate_data_types(decay_constant, (int, float), "decay_constant")

            df["weight"] = np.exp(decay_constant * df[time_diff])
            return df

        except Exception as e:
            raise Exception(f"Failed to calculate weight: {str(e)}")
