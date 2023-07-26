import emoji
import pandas as pd


class RecipientMapper:
    @staticmethod
    def create_recipient_id_to_name_mapping(df_recipient: pd.DataFrame) -> dict:
        """
        Creates a dictionary to map recipient_id to system_given_name.

        Args:
            df_recipient (pd.DataFrame): Pandas DataFrame containing recipient information.
                It should have columns '_id' and 'profile_joined_name'.

        Returns:
            dict: A dictionary mapping recipient_id to system_given_name.

        Raises:
            ValueError: If df_recipient is not a pandas DataFrame or if it is missing required columns.
        """
        # Validate the input argument.
        if not isinstance(df_recipient, pd.DataFrame):
            raise TypeError("df_recipient must be a pandas DataFrame")

        required_columns = {"_id", "profile_joined_name"}
        if not required_columns.issubset(df_recipient.columns):
            raise ValueError(
                "df_recipient is missing required columns '_id' and/or 'profile_joined_name'"
            )

        recipient_id_to_name = df_recipient.set_index("_id")[
            "profile_joined_name"
        ].to_dict()
        return recipient_id_to_name


class DataFrameWrangler:
    """
    A class that includes functions to wrangle DataFrames.
    """

    @staticmethod
    def filter_and_rename_messages_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters and renames columns of a given DataFrame representing messages.

        Args:
            df (pd.DataFrame): The original DataFrame to be filtered and renamed.

        Returns:
            pd.DataFrame: A new DataFrame with the filtered and renamed columns.

        Raises:
            ValueError: If dataframe is not a pandas DataFrame.
        """
        # Check if the input is a DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValueError("dataframe must be a pandas DataFrame")

        # Renaming columns
        renamed_df = df.rename(
            columns={
                "_id": "comment_id",
                "thread_id": "comment_thread_id",
                "from_recipient_id": "comment_from_recipient_id",
                "date_sent": "comment_date_sent",
                "body": "comment_body",
            }
        )

        # Columns to filter
        columns_to_filter = [
            "comment_id",
            "comment_thread_id",
            "comment_from_recipient_id",
            "quote_id",
            "comment_date_sent",
            "comment_body",
        ]

        # Selecting the filtered columns and creating a copy
        select_df = renamed_df[columns_to_filter].copy()

        return select_df

    @staticmethod
    def filter_and_rename_reactions_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters and renames columns of a given DataFrame representing reactions.

        Args:
            df (pd.DataFrame): The original DataFrame to be filtered and renamed.

        Returns:
            pd.DataFrame: A new DataFrame with the filtered and renamed columns.

        Raises:
            ValueError: If dataframe is not a pandas DataFrame.
        """
        # Check if the input is a DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValueError("dataframe must be a pandas DataFrame")

        # Renaming columns
        renamed_df = df.rename(
            columns={
                "_id": "reaction_id",
                "author_id": "reaction_author_id",
                "date_sent": "reaction_date_sent",
            }
        )

        # Columns to filter
        columns_to_filter = [
            "reaction_id",
            "message_id",
            "reaction_author_id",
            "emoji",
            "reaction_date_sent",
        ]

        # Selecting the filtered columns and creating a copy
        select_df = renamed_df[columns_to_filter].copy()

        # Translating emojis in the "emoji" column and creating a new column "reaction_translation"
        select_df["reaction_translation"] = select_df["emoji"].apply(
            DataFrameWrangler.translate_emoji
        )

        return select_df

    @staticmethod
    def translate_emoji(text: str) -> str:
        """
        Translates emojis in the input text to their textual representation.

        Args:
            text (str): Input text containing emojis.

        Returns:
            str: Text with emojis replaced by their textual representation.
                 Untranslatable emojis will be replaced by '<emoji_not_translated>'.

        Raises:
            TypeError: If the input `text` is not a string.

        Example:
            >>> translate_emoji("I love Python! 😍🐍")
            'I love Python! :heart_eyes::snake:'
        """
        if not isinstance(text, str):
            raise TypeError("Input 'text' must be a string.")

        translated_text = []
        for emo in emoji.demojize(text).split():
            try:
                translated_emo = emoji.emojize(emo)
                translated_text.append(translated_emo)
            except AttributeError:
                # If an emoji doesn't have a textual representation, replace it with a placeholder.
                translated_text.append("<emoji_not_translated>")

        return " ".join(translated_text)
