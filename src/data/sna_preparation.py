from typing import List, Optional

import pandas as pd
from faker import Faker

from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)
from src.data.data_wrangling import (
    DateTimeConverter,
    EmojiDataWrangler,
    MessageDataWrangler,
    QuotationResponseDataWrangler,
)
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
            ValeError: If input df is missing required columns.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)
        validate_columns_in_dataframe(
            df,
            [
                "comment_from_recipient_id",
                "comment_date_sent_datetime",
                "response_from_recipient_id",
                "response_date_sent_datetime",
                "weight",
            ],
        )

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
            ValeError: If input df is missing required columns.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)
        validate_columns_in_dataframe(
            df,
            [
                "comment_from_recipient_id",
                "comment_date_sent_datetime",
                "emoji_author_id",
                "emoji_date_sent_datetime",
                "weight",
            ],
        )

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
            ValeError: If input df is missing required columns.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)
        validate_columns_in_dataframe(
            df,
            [
                "quotation_from_recipient_id",
                "quotation_date_sent_datetime",
                "response_from_recipient_id",
                "response_date_sent_datetime",
                "weight",
            ],
        )

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
        Never make a function like this again. Next time, plan out column names better in advance.

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
                    message_slim_df, emoji_slim_df
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
            if interaction_type == "quotation":
                time_col_1 = "response_date_sent"
                time_col_2 = "quotation_date_sent"
            else:
                time_col_1 = interaction_type + "_date_sent"
                time_col_2 = "comment_date_sent"

            processed_df = TimeCalculations.calculate_time_diff(
                processed_df, time_col_1, time_col_2
            )
            decay_constant = TimeCalculations.calculate_decay_constant(
                processed_df, "time_diff"
            )
            processed_df = TimeCalculations.calculate_weight(
                processed_df, decay_constant, base_value
            )

            # Convert Unix timestamps to readable datetime formats
            processed_df = DateTimeConverter.convert_unix_to_datetime(
                processed_df, time_col_2
            )
            processed_df = DateTimeConverter.convert_unix_to_datetime(
                processed_df, time_col_1
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

    @staticmethod
    def concatenate_dataframes_vertically(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Concatenate a list of pandas DataFrames vertically after ensuring they all have the same columns.

        Args:
            dfs (List[pd.DataFrame]): List of pandas DataFrames to concatenate.

        Returns:
            pd.DataFrame: The concatenated DataFrame.

        Raises:
            ValueError: If not all DataFrames have the same column names.
        """
        # Validate input data types
        validate_data_types(dfs, List, "dfs")

        try:
            # Check if all dataframes have the same columns
            columns_set = set(dfs[0].columns)
            for df in dfs[1:]:
                if set(df.columns) != columns_set:
                    raise ValueError("Not all DataFrames have the same column names.")

            # Concatenate dataframes vertically
            concatenated_df = pd.concat(dfs, axis=0, ignore_index=True)

            return concatenated_df

        except Exception as e:
            raise Exception(
                f"An error occurred while processing concatenating dataframes data: {e}"
            )


class NodesEdgesDataProcessor:
    def __init__(self):
        self.fake = Faker()

    def fake_nodes_edges_dataframe(self):
        n = 100
        data = {
            "target_participant_id": [
                self.fake.random_int(min=1, max=10) for _ in range(n)
            ],
            "target_datetime": [self.fake.date_this_decade() for _ in range(n)],
            "source_participant_id": [
                self.fake.random_int(min=1, max=10) for _ in range(n)
            ],
            "source_datetime": [self.fake.date_this_decade() for _ in range(n)],
            "weight": [self.fake.random_number(digits=2) for _ in range(n)],
            "interaction_category": [
                self.fake.random_element(elements=("response", "quotation", "emoji"))
                for _ in range(n)
            ],
        }
        return pd.DataFrame(data)

    @staticmethod
    def filter_dataframe_by_dates(df, start_date, end_date):
        df["target_datetime"] = pd.to_datetime(df["target_datetime"])
        df["source_datetime"] = pd.to_datetime(df["source_datetime"])
        mask = (df["target_datetime"] >= start_date) & (
            df["target_datetime"] <= end_date
        )
        return df[mask]
