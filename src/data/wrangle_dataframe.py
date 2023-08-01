import warnings

import emoji
import numpy as np
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
        slim_df = renamed_df[columns_to_filter].copy()

        return slim_df

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
        slim_df = renamed_df[columns_to_filter].copy()

        # Translating emojis in the "emoji" column and creating a new column "reaction_translation"
        slim_df["reaction_translation"] = slim_df["emoji"].apply(
            DataFrameWrangler.translate_emoji
        )

        return slim_df

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

    @staticmethod
    def create_quotation_response_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a DataFrame containing both quotations and responses from a message DataFrame.

        Args:
            df (pd.DataFrame): The original DataFrame containing messages.

        Returns:
            pd.DataFrame: A new DataFrame containing both quotations and responses.

        Raises:
            TypeError: If dataframe is not a pandas DataFrame.
        """
        # Check if the input is a DataFrame
        if not isinstance(df, pd.DataFrame):
            raise TypeError("dataframe must be a pandas DataFrame")

        # Renaming columns for quotations
        quotation_df = df.rename(
            columns={
                "_id": "quotation_id",
                "date_sent": "quotation_date_sent",
                "thread_id": "quotation_thread_id",
                "from_recipient_id": "quotation_from_recipient_id",
                "body": "quotation_body",
            }
        )

        # Renaming columns for responses
        response_df = df.copy().rename(
            columns={
                "_id": "response_id",
                "date_sent": "response_date_sent",
                "thread_id": "response_thread_id",
                "from_recipient_id": "response_from_recipient_id",
                "body": "response_body",
            }
        )

        # Convert quote_id to datetime64
        response_df["quote_id"] = pd.to_datetime(response_df["quote_id"], unit="ns")

        # Merge quotations and responses based on date_sent
        quotation_response_df = quotation_df.merge(
            response_df, left_on="quotation_date_sent", right_on="quote_id"
        )

        # Reset the index of the resulting dataframe
        quotation_response_df.reset_index(drop=True, inplace=True)

        return quotation_response_df

    @staticmethod
    def calculate_time_diff(
        df: pd.DataFrame, timestamp1: str, timestamp2: str
    ) -> pd.DataFrame:
        """
        Calculates the time difference between two timestamps and stores the result in a new column 'time_diff'.
        The time difference is given in seconds. A warning is raised if any time difference is negative.

        Args:
            df (pd.DataFrame): DataFrame with at least two columns containing timestamps.
            timestamp1 (str): Name of the first timestamp column.
            timestamp2 (str): Name of the second timestamp column.

        Returns:
            pd.DataFrame: DataFrame with an additional 'time_diff' column.

        Raises:
            TypeError: If df is not a pandas DataFrame or timestamp1 and timestamp2 are not strings.
            ValueError: If timestamp1 and timestamp2 are not columns in df.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("dataframe must be a pandas DataFrame")
        if not isinstance(timestamp1, str) or not isinstance(timestamp2, str):
            raise TypeError("timestamp1 and timestamp2 must be strings")

        try:
            df["time_diff"] = df.apply(
                lambda row: 0
                if row[timestamp1] == row[timestamp2]
                else (row[timestamp1] - row[timestamp2]) / 1000,
                axis=1,
            )
        except KeyError as e:
            raise KeyError(f"Could not find column {e.args[0]} in DataFrame")

        if (df["time_diff"] < 0).any():
            warnings.warn(
                "There are negative time differences in the data", UserWarning
            )

        return df

    @staticmethod
    def calculate_decay_constant(
        df: pd.DataFrame, column: str, percentile: float = 0.75
    ) -> float:
        """
        Calculates the decay constant based on a specified percentile of a specified column in the given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with at least one numerical column.
            column (str): Name of the column based on which the decay constant is to be calculated.
            percentile (float, optional): Percentile value to be used in the decay constant calculation.
                Must be between 0 and 1 (exclusive). Defaults to 0.75.

        Returns:
            float: Decay constant calculated as log(2) divided by the specified percentile of the specified column.

        Raises:
            TypeError: If df is not a pandas DataFrame, column is not a string, or percentile is not a float.
            ValueError: If column is not a column in df or percentile is not between 0 and 1 (exclusive).
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"Expected 'df' to be a pandas DataFrame, but got {type(df)}"
            )
        if not isinstance(column, str):
            raise TypeError(f"Expected 'column' to be a string, but got {type(column)}")
        if not isinstance(percentile, float) or not 0 <= percentile < 1:
            raise ValueError(
                f"Expected 'percentile' to be a float between 0 and 1 (exclusive), but got {percentile}"
            )

        try:
            half_life = df[column].quantile(percentile)
            decay_constant = np.log(2) / half_life
        except KeyError as e:
            raise KeyError(f"Could not find column {e.args[0]} in DataFrame")

        return decay_constant

    @staticmethod
    def calculate_weight(
        df: pd.DataFrame, decay_constant: float, base_value: float
    ) -> pd.DataFrame:
        """
        Calculates the weight using the exponential decay function.

        Args:
            df (pd.DataFrame): DataFrame with at least a numerical column 'time_diff'.
            decay_constant (float): Decay constant for the calculation.
            base_value (float): Base value for the weight calculation.

        Returns:
            pd.DataFrame: Original DataFrame with an additional 'weight' column.

        Raises:
            TypeError: If df is not a pandas DataFrame, decay_constant is not a float, or base_value is not a float.
            KeyError: If 'time_diff' column is not in df.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"Expected 'df' to be a pandas DataFrame, but got {type(df)}"
            )
        if not isinstance(decay_constant, float):
            raise TypeError(
                f"Expected 'decay_constant' to be a float, but got {type(decay_constant)}"
            )
        if not isinstance(base_value, float):
            raise TypeError(
                f"Expected 'base_value' to be a float, but got {type(base_value)}"
            )

        try:
            df["weight"] = base_value * np.exp(-decay_constant * df["time_diff"])
        except KeyError as e:
            raise KeyError(f"Could not find column {e.args[0]} in DataFrame")

        return df
