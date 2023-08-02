import pandas as pd
from src.data.data_validation import validate_columns_in_dataframe, validate_dataframe
from emoji_translation import EmojiTranslator


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
            Exception: If there's an error during the filtering and renaming process.
        """
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
            Exception: If there's an error during the filtering and renaming process.
        """
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
            Exception: If there's an error during the creation of the quotation response DataFrame.
        """
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
