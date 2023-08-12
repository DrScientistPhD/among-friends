import pandas as pd
import polars as pl
from polars import col

from src.data.data_validation import (validate_columns_in_dataframe,
                                      validate_data_types, validate_dataframe)
from src.data.emoji_translation import EmojiTranslator


class DateTimeConverter:
    @staticmethod
    def convert_unix_to_datetime(
        df: pd.DataFrame, timestamp_column: str
    ) -> pd.DataFrame:
        """
        Converts a column with Unix timestamps (in milliseconds) to a more readable datetime format.

        Args:
            df (pd.DataFrame): The DataFrame containing the Unix timestamp column.
            timestamp_column (str): The name of the column containing Unix timestamps in milliseconds.

        Returns:
            pd.DataFrame: A DataFrame with a new column containing the converted datetimes.

        Raises:
            TypeError: If df is not a pandas DataFrame, or timestamp_column is not a string.
            KeyError: If the specified timestamp column does not exist in the DataFrame.
            ValueError: If an unexpected error occurs during datetime conversion.
        """
        # Validate input data
        validate_dataframe(df)
        validate_data_types(timestamp_column, str, "timestamp_column")
        validate_columns_in_dataframe(df, [timestamp_column])

        try:
            # Check if the timestamp column exists in the DataFrame
            if timestamp_column not in df.columns:
                raise KeyError(
                    f"The specified column '{timestamp_column}' does not exist in the DataFrame."
                )

            # Check if the timestamp column is of integer type
            if not pd.api.types.is_integer_dtype(df[timestamp_column]):
                raise TypeError(
                    f"The specified column '{timestamp_column}' "
                    f"must be of integer type representing Unix timestamps in milliseconds."
                )

            # Convert the Unix timestamps to datetime format
            new_column_name = f"{timestamp_column}_datetime"
            df[new_column_name] = pd.to_datetime(df[timestamp_column], unit="ms")

            return df

        except Exception as e:
            raise Exception(
                f"An error occurred while converting the Unix timestamps to datetime: {str(e)}"
            )


class MessageDataWrangler:
    @staticmethod
    def filter_and_rename_messages_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters and renames columns of a given DataFrame representing messages.

        Args:
            df (pd.DataFrame): The original DataFrame to be filtered and renamed.

        Returns:
            pd.DataFrame: A new DataFrame with the filtered and renamed columns.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            KeyError: If the specified timestamp column does not exist in the DataFrame.
            Exception: If there's an error during the filtering and renaming process.
        """
        # Validate input data types
        validate_dataframe(df)
        validate_columns_in_dataframe(
            df, ["_id", "thread_id", "from_recipient_id", "date_sent", "body"]
        )

        try:
            renamed_df = df.rename(
                columns={
                    "_id": "comment_id",
                    "thread_id": "comment_thread_id",
                    "from_recipient_id": "comment_from_recipient_id",
                    "date_sent": "comment_date_sent",
                    "body": "comment_body",
                }
            )

            columns_to_filter = [
                "comment_id",
                "comment_thread_id",
                "comment_from_recipient_id",
                "quote_id",
                "comment_date_sent",
                "comment_body",
            ]

            slim_df = renamed_df[columns_to_filter].copy()

            return slim_df

        except Exception as e:
            raise Exception(f"Failed to filter and rename messages DataFrame: {str(e)}")

    @staticmethod
    def concatenate_comment_threads(
        df_pd: pd.DataFrame, group_participants_n: int
    ) -> pd.DataFrame:
        """
        Concatenates comment threads from a pandas DataFrame, taking the i-th row and
        concatenating it with the next rows that match the criteria. Filters responses based on
        comment_from_recipient_id, takes the first n responses, and concatenates them horizontally
        with the original comment row. Responses are filtered to exclude cases such that the comment_from_recipient_id
        cannot be equal to the response_from_recipient_id (excludes instances where a person responds to themselves).

        Args:
            df_pd (pd.DataFrame): Input pandas DataFrame with columns representing comment threads.
            group_participants_n (int): Maximum number of responses to include from the group participants for each
                thread.

        Returns:
            pd.DataFrame: Final processed pandas DataFrame, with original comments and selected responses.

        Raises:
            TypeError: If df is not a pandas DataFrame, or group_participants_n is not an int.
            ValueError: If an unexpected error occurs during conversion.
            Exception: If there's an error while concatenating comment threads.
        """
        # Validate input data types
        validate_dataframe(df_pd)
        validate_data_types(group_participants_n, int, "group_participants_n")

        try:
            # Convert the pandas DataFrame to a Polars DataFrame
            df_polars = pl.DataFrame(df_pd)

            # Define a mapping for column renaming
            rename_mapping = {
                "comment_id": "response_id",
                "comment_date_sent": "response_date_sent",
                "comment_from_recipient_id": "response_from_recipient_id",
                "comment_body": "response_body",
            }

            dfs = []

            # Iterate over the rows of the dataframe
            for i in range(len(df_polars)):
                current_row_slice = df_polars.slice(i, 1)
                next_rows = df_polars.slice(i + 1, len(df_polars) - i - 1)
                next_rows = next_rows.filter(
                    col("comment_from_recipient_id")
                    != df_polars[i]["comment_from_recipient_id"]
                )
                next_rows = next_rows.slice(
                    0, min(group_participants_n, len(next_rows))
                )

                for j in range(len(next_rows)):
                    # Rename the columns of the next row before concatenation
                    next_row = next_rows.slice(j, 1).select(
                        [
                            col(name).alias(rename_mapping[name])
                            for name in rename_mapping
                        ]
                    )

                    # Stack the current row and the next row horizontally
                    new_df = pl.concat([current_row_slice, next_row], how="horizontal")
                    dfs.append(new_df)

            # Concatenate all the dataframes vertically to get the final dataframe
            df_final_polars = pl.concat(dfs, how="vertical")

            # Convert the final Polars DataFrame back to a pandas DataFrame
            df_final_pd = df_final_polars.to_pandas()
            return df_final_pd

        except Exception as e:
            raise ValueError(
                f"An error occurred while concatenating the comment threads: {str(e)}"
            )


class ReactionDataWrangler:
    @staticmethod
    def filter_and_rename_reactions_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters and renames columns of a given DataFrame representing reactions.

        Args:
            df (pd.DataFrame): The original DataFrame to be filtered and renamed.

        Returns:
            pd.DataFrame: A new DataFrame with the filtered and renamed columns.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            KeyError: If the specified columns do not exist in the DataFrame.
            Exception: If there's an error during the filtering and renaming process.
        """
        # Validate input data types
        validate_dataframe(df)
        validate_columns_in_dataframe(df, ["_id", "author_id", "date_sent", "emoji"])

        try:
            renamed_df = df.rename(
                columns={
                    "_id": "reaction_id",
                    "author_id": "reaction_author_id",
                    "date_sent": "reaction_date_sent",
                }
            )

            columns_to_filter = [
                "reaction_id",
                "message_id",
                "reaction_author_id",
                "emoji",
                "reaction_date_sent",
            ]

            slim_df = renamed_df[columns_to_filter].copy()

            slim_df["reaction_translation"] = slim_df["emoji"].apply(
                EmojiTranslator.translate_emoji
            )

            return slim_df

        except Exception as e:
            raise Exception(
                f"Failed to filter and rename reactions DataFrame: {str(e)}"
            )


class QuotationResponseDataWrangler:
    @staticmethod
    def create_quotation_response_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a DataFrame containing both quotations and responses from a message DataFrame.

        Args:
            df (pd.DataFrame): The original DataFrame containing messages.

        Returns:
            pd.DataFrame: A new DataFrame containing both quotations and responses.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            KeyError: If the specified columns do not exist in the DataFrame.
            Exception: If there's an error during the creation of the quotation response DataFrame.
        """
        # Validate input data types
        validate_dataframe(df)
        validate_columns_in_dataframe(
            df, ["_id", "date_sent", "thread_id", "from_recipient_id", "body"]
        )

        try:
            quotation_df = df.rename(
                columns={
                    "_id": "quotation_id",
                    "date_sent": "quotation_date_sent",
                    "thread_id": "quotation_thread_id",
                    "from_recipient_id": "quotation_from_recipient_id",
                    "body": "quotation_body",
                }
            )

            response_df = df.copy().rename(
                columns={
                    "_id": "response_id",
                    "date_sent": "response_date_sent",
                    "thread_id": "response_thread_id",
                    "from_recipient_id": "response_from_recipient_id",
                    "body": "response_body",
                }
            )

            response_df["quote_id"] = pd.to_datetime(response_df["quote_id"], unit="ns")

            quotation_response_df = quotation_df.merge(
                response_df, left_on="quotation_date_sent", right_on="quote_id"
            )

            quotation_response_df.reset_index(drop=True, inplace=True)

            return quotation_response_df

        except Exception as e:
            raise Exception(f"Failed to create quotation response DataFrame: {str(e)}")
