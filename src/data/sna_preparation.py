from typing import Optional

import pandas as pd

from src.data.data_validation import validate_data_types, validate_dataframe
from src.data.data_wrangling import (DateTimeConverter, EmojiDataWrangler,
                                     MessageDataWrangler,
                                     QuotationResponseDataWrangler)
from src.data.time_calculations import TimeCalculations


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

    @staticmethod
    def process_data_for_sna(
        interaction_type: str,
        base_value: float,
        message_slim_df: pd.DataFrame,
        emoji_slim_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Process data based on the data type.

        Args:
            interaction_type (str): The type of interaction to be processed. One of "message", "emoji", "quotation".
            base_value (float): Base value used to derive the weight of interactions.
            message_slim_df (DataFrame): A preprocessed message dataframe.
            emoji_slim_df (Optional[DataFrame]): A preprocessed emoji dataframe. Required only if interaction_type is "emoji".

        Returns:
            DataFrame: The processed dataframe.

        Raises:
            TypeError: If the interaction_type is not a str, or the base_value is not a float, or the dataframes are not valid.
            ValueError: If the provided `data_type` is not one of "response", "emoji", "quotation".
            Exception: For other exceptions during processing.
        """

        # Validate input data types
        validate_data_types(interaction_type, str, "interaction_type")
        validate_data_types(base_value, float, "base_value")
        validate_dataframe(message_slim_df)

        if interaction_type == "emoji":
            validate_dataframe(emoji_slim_df)

        try:
            if interaction_type == "response":
                group_n = message_slim_df["comment_from_recipient_id"].nunique()
                processed_df = MessageDataWrangler.concatenate_comment_threads(
                    message_slim_df, group_n
                )
            elif interaction_type == "emoji":
                processed_df = EmojiDataWrangler.merge_message_with_emoji(
                    emoji_slim_df, message_slim_df
                )
            elif interaction_type == "quotation":
                processed_df = (
                    QuotationResponseDataWrangler.create_quotation_response_df(
                        message_slim_df
                    )
                )
            else:
                raise ValueError(
                    f"Invalid data_type: {interaction_type}. Expected one of ['response', 'emoji', 'quotation']."
                )

            # Calculate time differences, decay constants, and weights
            time_col = interaction_type + "_date_sent"
            processed_df = TimeCalculations.calculate_time_diff(
                processed_df, time_col, "comment_date_sent"
            )
            decay_constant = TimeCalculations.calculate_decay_constant(
                processed_df, "time_diff"
            )
            processed_df = TimeCalculations.calculate_weight(
                processed_df, decay_constant, base_value
            )

            # Convert Unix timestamps to readable datetime formats
            processed_df = DateTimeConverter.convert_unix_to_datetime(
                processed_df, "comment_date_sent"
            )
            processed_df = DateTimeConverter.convert_unix_to_datetime(
                processed_df, time_col
            )

            # Standardize dataframe
            if interaction_type == "response":
                return SnaDataWrangler.standardize_response_react_dataframe(
                    processed_df
                )
            elif interaction_type == "emoji":
                return SnaDataWrangler.standardize_emoji_react_dataframe(processed_df)
            elif interaction_type == "quotation":
                return SnaDataWrangler.standardize_quotation_react_dataframe(
                    processed_df
                )

        except Exception as e:
            raise Exception(
                f"An error occurred while processing {interaction_type} data: {e}"
            )
