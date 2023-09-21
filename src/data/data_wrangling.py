import pandas as pd
import polars as pl
from polars import col

from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)
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
            Exception: If an error occurs converting unix to datetime.
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
    def filter_and_rename_messages_df(df: pd.DataFrame, thread_id: int) -> pd.DataFrame:
        """
        Filters and renames columns of a given DataFrame representing messages.

        Args:
            df (pd.DataFrame): The original DataFrame to be filtered and renamed.
            thread_id (int): The thread_id to filter by.

        Returns:
            pd.DataFrame: A new DataFrame with the filtered and renamed columns.

        Raises:
            TypeError: If df is not a pandas DataFrame, or thread_id is not an int.
            KeyError: If the specified timestamp column does not exist in the DataFrame.
            ValueError: If there are no records that meet the specified thread_id filter.
            Exception: If there's an error during the filtering and renaming process.
        """
        # Validate input data types
        validate_dataframe(df)
        validate_data_types(thread_id, int, "thread_id")
        validate_columns_in_dataframe(
            df, ["_id", "thread_id", "from_recipient_id", "date_sent", "body"]
        )

        # Check if there are any records that meet the specified thread_id filter
        if df[df["thread_id"] == thread_id].empty:
            raise ValueError(f"No records found for thread_id: {thread_id}")

        try:
            df = df[df["thread_id"] == thread_id]

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

            renamed_df["quote_id"] = renamed_df["quote_id"].fillna(0)
            renamed_df["quote_id"] = renamed_df["quote_id"].astype(int)

            slim_df = renamed_df[columns_to_filter].copy()

            return slim_df

        except Exception as e:
            raise Exception(f"Failed to filter and rename messages DataFrame: {str(e)}")

    @staticmethod
    def concatenate_comment_threads(
        df_pd: pd.DataFrame, group_participants_n: int
    ) -> pd.DataFrame:
        """
        Concatenates comment threads from a pandas DataFrame, taking the i-th row and concatenating it with the next
        rows that match the criteria. Filters responses based on comment_from_recipient_id, takes the first n
        responses, and concatenates them horizontally with the original comment row. Responses are filtered to
        exclude cases such that the comment_from_recipient_id cannot be equal to the response_from_recipient_id (
        excludes instances where a person responds to themselves).

        Args:
            df_pd (pd.DataFrame): Input pandas DataFrame with columns representing comment threads.
            group_participants_n (int): Maximum number of responses to include from the group participants for each
                thread.

        Returns:
            pd.DataFrame: Final processed pandas DataFrame, with original comments and selected responses.

        Raises:
            TypeError: If input df is not a pandas DataFrame, or group_participants_n is not an int.
            KeyError: If the specified columns do not exist in the DataFrame.
            ValueError: If there's an error while concatenating comment threads.
            Exception: If an error occurs concatenating comment threads.
        """
        # Validate input data types
        validate_dataframe(df_pd)
        validate_columns_in_dataframe(
            df_pd,
            [
                "comment_id",
                "comment_date_sent",
                "comment_from_recipient_id",
                "comment_body",
            ],
        )
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

            # Sort dataframe by comment_date_set to ensure proper comment-response linkage
            df_polars = df_polars.sort(by="comment_date_sent")

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


class EmojiDataWrangler:
    @staticmethod
    def filter_and_rename_emojis_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters and renames columns of a given DataFrame representing emojis.

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
                    "_id": "emoji_id",
                    "author_id": "emoji_author_id",
                    "date_sent": "emoji_date_sent",
                }
            )

            columns_to_filter = [
                "emoji_id",
                "message_id",
                "emoji_author_id",
                "emoji",
                "emoji_date_sent",
            ]

            slim_df = renamed_df[columns_to_filter].copy()

            slim_df["emoji_translation"] = slim_df["emoji"].apply(
                EmojiTranslator.translate_emoji
            )

            return slim_df

        except Exception as e:
            raise Exception(f"Failed to filter and rename emojis DataFrame: {str(e)}")

    @staticmethod
    def merge_message_with_emoji(
        message_df: pd.DataFrame, emoji_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merges two DataFrames, one containing message details and the other containing emoji details, based on the
        'comment_id' in the message DataFrame and the 'message_id' in the emoji DataFrame.

        Args:
            message_df (pd.DataFrame): DataFrame containing message details, with a 'comment_id' column.
            emoji_df (pd.DataFrame): DataFrame containing emoji details, with a 'message_id' column.

        Returns:
            pd.DataFrame: Merged DataFrame containing both message and emoji details.

        Raises:
            TypeError: If either of the input DataFrames is not a pandas DataFrame.
            KeyError: If the required columns ('comment_id' or 'message_id') are not present in the input DataFrames.
            Exception: If there's an error during the merging process.
        """
        # Validate input data
        validate_dataframe(message_df)
        validate_dataframe(emoji_df)
        validate_columns_in_dataframe(message_df, ["comment_id"])
        validate_columns_in_dataframe(emoji_df, ["message_id"])

        try:
            # Merge message and emoji dataframes on the specified columns
            df_message_emoji = pd.merge(
                message_df,
                emoji_df,
                left_on="comment_id",
                right_on="message_id",
                suffixes=("", "_emoji"),
            )
            return df_message_emoji

        except Exception as e:
            raise Exception(f"Failed to merge message and emoji dataframes: {str(e)}")


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
            df,
            [
                "comment_id",
                "comment_thread_id",
                "comment_from_recipient_id",
                "quote_id",
                "comment_date_sent",
                "comment_body",
            ],
        )

        try:
            quotation_df = df.rename(
                columns={
                    "comment_id": "quotation_id",
                    "comment_date_sent": "quotation_date_sent",
                    "comment_thread_id": "quotation_thread_id",
                    "comment_from_recipient_id": "quotation_from_recipient_id",
                    "comment_body": "quotation_body",
                }
            )

            response_df = df.copy().rename(
                columns={
                    "comment_id": "response_id",
                    "comment_date_sent": "response_date_sent",
                    "comment_thread_id": "response_thread_id",
                    "comment_from_recipient_id": "response_from_recipient_id",
                    "comment_body": "response_body",
                }
            )

            # Oddly enough, the quote_id is a timestamp that ties a response back to the original message (the #
            # quotation) # After the merge, drop one of the duplicate quote_id columns and rename the other back to
            # its original name.
            quotation_response_df = quotation_df.merge(
                response_df, left_on="quotation_date_sent", right_on="quote_id"
            )

            # Drop the quote_id_y column and rename quote_id_x to quote_id
            quotation_response_df.drop(columns=["quote_id_y"], inplace=True)
            quotation_response_df.rename(
                columns={"quote_id_x": "quote_id"}, inplace=True
            )

            quotation_response_df.reset_index(drop=True, inplace=True)

            return quotation_response_df

        except Exception as e:
            raise Exception(f"Failed to create quotation response DataFrame: {str(e)}")
