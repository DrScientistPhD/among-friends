import pandas as pd
from typing import List

from src.data.data_validation import (validate_columns_in_dataframe, validate_data_types, validate_dataframe)


class SnaDataWrangler:
    @staticmethod
    def create_reacted_dataframe(df: pd.DataFrame, columns_to_keep: List, interaction_category: str) -> pd.DataFrame:
        """
        Creates a new DataFrame by subsetting columns and adding a reaction category.

        Args:
            df (pd.DataFrame): The input DataFrame.
            columns_to_keep (list): List of column names to keep in the DataFrame.
            interaction_category (str): The category name for the "interaction_category" column.

        Returns:
            pd.DataFrame: A new DataFrame with the specified columns and reaction category.

        Raise:

        """
        # Validate input data
        validate_dataframe(df)
        validate_data_types(interaction_category, str, "interaction_category")
        validate_data_types(columns_to_keep, List, "columns_to_keep")
        validate_columns_in_dataframe(df, columns_to_keep)

        try:
            new_data_frame = df[columns_to_keep].copy()
            new_data_frame["interaction_category"] = interaction_category
            return new_data_frame

        except Exception as e:
            raise Exception(f"Failed to subset and assign new category to input dataframe.")