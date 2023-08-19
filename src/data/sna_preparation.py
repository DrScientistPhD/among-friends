from typing import List

import pandas as pd

from src.data.data_validation import validate_dataframe


class SnaDataWrangler:
    @staticmethod
    def standardize_response_react_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a standardized dataframe for comment-response pairs by selecting only the relevant columns and adding
        a reaction category.

        Args:
            df (df.DataFrame): The input comment-response DataFrame.

        Returns:
            pd.DataFrame: The standardized comment-response DataFrame.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)

        try:
            df = df.rename(
                columns={
                    "comment_from_recipient_id": "target_participant_id",
                    "comment_date_sent_datetime": "target_datetime",
                    "response_from_recipient_id": "source_participant_id",
                    "response_date_sent_datetime": "source_datetime",
                }
            )

            columns_to_keep = [
                "target_participant_id",
                "target_datetime",
                "source_participant_id",
                "source_datetime",
                "weight",
            ]

            standardized_df = df[columns_to_keep].copy()

            standardized_df["interaction_category"] = "response"
            return standardized_df

        except Exception as e:
            raise Exception(
                f"Failed to subset and assign new category to input dataframe."
            )

    @staticmethod
    def standardize_emoji_react_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a standardized dataframe for comment-emoji pairs by selecting only the relevant columns and adding
        a reaction category.

        Args:
            df (df.DataFrame): The input comment-emoji DataFrame.

        Returns:
            pd.DataFrame: The standardized comment-emoji DataFrame.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)

        try:
            df = df.rename(
                columns={
                    "comment_from_recipient_id": "target_participant_id",
                    "comment_date_sent_datetime": "target_datetime",
                    "emoji_author_id": "source_participant_id",
                    "emoji_date_sent_datetime": "source_datetime",
                }
            )

            columns_to_keep = [
                "target_participant_id",
                "target_datetime",
                "source_participant_id",
                "source_datetime",
                "weight",
            ]

            standardized_df = df[columns_to_keep].copy()

            standardized_df["interaction_category"] = "emoji"
            return standardized_df

        except Exception as e:
            raise Exception(
                f"Failed to subset and assign new category to input dataframe."
            )


    @staticmethod
    def standardize_quotation_react_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a standardized dataframe for quotation-response pairs by selecting only the relevant columns and adding
        a reaction category.

        Args:
            df (df.DataFrame): The input quotation-response DataFrame.

        Returns:
            pd.DataFrame: The standardized quotation-response DataFrame.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)

        try:
            df = df.rename(
                columns={
                    "quotation_from_recipient_id": "target_participant_id",
                    "quotation_date_sent_datetime": "target_datetime",
                    "response_from_recipient_id": "source_participant_id",
                    "response_date_sent_datetime": "source_datetime",
                }
            )

            columns_to_keep = [
                "target_participant_id",
                "target_datetime",
                "source_participant_id",
                "source_datetime",
                "weight",
            ]

            standardized_df = df[columns_to_keep].copy()

            standardized_df["interaction_category"] = "quotation"
            return standardized_df

        except Exception as e:
            raise Exception(
                f"Failed to subset and assign new category to input dataframe."
            )