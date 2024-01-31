from typing import List

import numpy as np
import pandas as pd
import phonenumbers
from phonenumbers import geocoder

from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)
from src.data.data_wrangling import DateTimeConverter
from src.data.recipient_mapper import RecipientMapper


class ProcessMessageData:
    def __init__(self, recipient_mapper_instance: RecipientMapper):
        self.recipient_mapper_instance = recipient_mapper_instance

    @staticmethod
    def filter_long_text(text: str) -> str:
        """
        Removes unusually long text from a string and replaces it with a single white space ('').

        Args:
            text (str): The text to filter.

        Returns:
            str: The filtered text.

        Raises:
            TypeError: If text is not of type str.
        """
        validate_data_types(text, str, "text")

        try:
            return (
                " ".join(
                    word for word in (text.split() or [""]) if len(word or "") <= 25
                )
                if text is not None
                else None
            )

        except Exception as e:
            raise Exception(f"Failed to filter long text: {str(e)}")

    def clean_up_messages(
        self, message_df: pd.DataFrame, recipient_df: pd.DataFrame, thread_id: int
    ) -> pd.DataFrame:
        """
        Processes the messages CSV file for GenAI usage. Data processing includes converting column types,
        converting datetime data, changing column names, filling in missing values, and removing extraneous text,
        mapping IDs to usernames, removing empty rows, and selecting appropriate columns.

        Args:
            message_df (pd.DataFrame): The original message DataFrame to be processed.
            recipient_df (pd.DataFrame): The recipient DataFrame to be used for mapping recipient IDs to usernames.
            thread_id (int): The thread_id to filter by

        Returns:
            pd.DataFrame: A new DataFrame with the filtered and renamed columns.

        Raises:
            TypeError: If message_df or recipient_df are not pandas DataFrames, or thread_id is not an int.
            KeyError: If the specified columns do not exist in the DataFrames.
            ValueError: If there are no records that meet the specified thread_id filter.
            Exception: If there's an error during processing.
        """
        # Validate input data types
        validate_dataframe(message_df)
        validate_dataframe(recipient_df)
        validate_data_types(thread_id, int, "thread_id")
        validate_columns_in_dataframe(
            message_df,
            [
                "date_sent",
                "thread_id",
                "from_recipient_id",
                "body",
                "quote_id",
                "quote_author",
                "quote_body",
            ],
        )
        validate_columns_in_dataframe(recipient_df, ["system_display_name", "phone"])

        # Check if there are any records that meet the specified thread_id filter
        if message_df[message_df["thread_id"] == thread_id].empty:
            raise ValueError(f"No records found for thread_id: {thread_id}")

        try:
            # Filter for the specified thread_id
            message_df = message_df[message_df["thread_id"] == thread_id].copy()

            # Sorting the DataFrame in place
            message_df.sort_values(by="date_sent", ascending=True, inplace=True)

            # Convert datetime column to an easily read format
            processed_data = DateTimeConverter.convert_unix_to_date_info(
                message_df, "date_sent"
            )

            # Fill in missing values for the quote_author column for easier data processing and select columns
            processed_data["quote_author"] = (
                processed_data["quote_author"].fillna(0).astype(int).replace(0, "")
            )

            # Select for appropriate columns and sort by date_sent_datetime
            message_columns = [
                "sent_year",
                "sent_month",
                "sent_day",
                "sent_day_of_week",
                "from_recipient_id",
                "body",
                "quote_author",
                "quote_body",
            ]

            # Remove rows with an empty body
            processed_data = processed_data[processed_data["body"].notnull()]

            selected_data = processed_data[message_columns].copy()

            recipient_id_to_name_dict = (
                self.recipient_mapper_instance.create_recipient_id_to_name_mapping(
                    recipient_df, "system_display_name"
                )
            )

            selected_data["from_recipient_id"] = selected_data["from_recipient_id"].map(
                recipient_id_to_name_dict
            )
            selected_data["quote_author"] = selected_data["quote_author"].map(
                recipient_id_to_name_dict
            )

            # Replace NaN values with None throughout the DataFrame and rename columns
            selected_data.replace(np.nan, "", inplace=True)
            selected_data.rename(
                columns={"from_recipient_id": "message_author"}, inplace=True
            )

            processed_message_df = selected_data.copy()

            # Removes unusually long text and empty body rows from the "body" and "quote_body" columns
            processed_message_df["body"] = processed_message_df["body"].apply(
                ProcessMessageData.filter_long_text
            )
            processed_message_df = processed_message_df[
                ~processed_message_df["body"].str.strip().eq("")
            ]

            processed_message_df["quote_body"] = processed_message_df[
                "quote_body"
            ].apply(ProcessMessageData.filter_long_text)

            return processed_message_df

        except Exception as e:
            raise Exception(
                f"Failed to process messages DataFrame for GenAI purposes: {str(e)}"
            )

    @staticmethod
    def processed_message_data_to_sentences(
        processed_message_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Converts each row of the processed message DataFrame to a standardized sentence format and returns a DataFrame.

        Args:
            processed_message_df (pd.DataFrame): The processed message DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing standardized sentences.

        Raises:
        """
        # Validate input data types
        validate_dataframe(processed_message_df)
        validate_columns_in_dataframe(
            processed_message_df,
            [
                "sent_year",
                "sent_month",
                "sent_day",
                "sent_day_of_week",
                "message_author",
                "body",
                "quote_author",
                "quote_body",
            ],
        )

        try:
            result_df = pd.DataFrame()
            result_df["sentence"] = processed_message_df.apply(
                lambda row: (
                    ' '.join([
                        row['message_author'], "said", f'"{row["body"]}".'
                    ]) if row["quote_author"].strip() == "" else ' '.join([
                        row['message_author'], "said", f'"{row["body"]}".',
                        "This message was quoting and responding to",
                        f'{row["quote_author"]}', "who said", f'"{row["quote_body"]}".'
                    ])
                ),
                axis=1,
            )

            # Copy additional columns from processed_message_df to result_df
            result_df["sent_year"] = processed_message_df["sent_year"]
            result_df["sent_month"] = processed_message_df["sent_month"]
            result_df["sent_day"] = processed_message_df["sent_day"]
            result_df["sent_day_of_week"] = processed_message_df["sent_day_of_week"]

            return result_df

        except Exception as e:
            raise Exception(
                f"Failed to convert processed message data to sentences: {str(e)}"
            )

    @staticmethod
    def concatenate_with_neighbors(message_sentences_df) -> pd.DataFrame:
        """
        Concatenates the values in a DataFrame with their neighboring rows so that each row contains the text from the
        previous row, the current row, and the next row. Concatenations are applied to the text column and are done
        using the following criteria:
        1. Rows are concatenated sequentially and no concatenated row exceeds more than 1,000 characters. If
        concatenating an additional row would exceed 1,000 characters, the current row is concatenated with the next
        group of rows.
        2. Only rows with the same sent_year, sent_month, sent_day, and sent_day_of_week values are concatenated. The
        concatenated row retains the earliest sent_year, sent_month, sent_day, and sent_day_of_week values from
        this grouping.
        3. There is a deliberate one row overlap between the final concatenated rows, such that the first and last
        sentence of a concatenated row are the same as the first and last sentence of the previous concatenated row
        and the next concatenated row, respectively.
        4. The first and final rows are not concatenated with the previous and next rows, respectively.
        5. Emojis are removed from the final concatenated text.

        Args:
            message_sentences_df (pd.DataFrame): A DataFrame containing the message sentences.

        Returns:
            pd.DataFrame: A pandas DataFrame with text messages.

        Raises:
            TypeError: If message_sentences_df is not a pandas DataFrame.
            KeyError: If the specified columns do not exist in the DataFrame.
            Exception: If there's an error during processing.
        """
        # Validate input data types
        validate_dataframe(message_sentences_df)

        # Validate that the DataFrame contains the necessary columns
        validate_columns_in_dataframe(message_sentences_df, ["sentence", "sent_year", "sent_month", "sent_day",
                                                             "sent_day_of_week"])

        try:
            # Initialize an empty list to store dataframes
            result_dfs = []

            current_texts = []
            current_group = None

            for index, row in message_sentences_df.iterrows():
                # Check if adding the current row would exceed 500 characters
                if sum(map(len, current_texts)) + len(row['sentence']) > 500:
                    # Create a new dataframe for the current group with sentence overlap
                    current_df = pd.DataFrame({'text': [" ".join(current_texts)], **current_group})

                    # Append the current_df to the list
                    result_dfs.append(current_df)

                    # Reset current_texts and update current_group with sentence overlap
                    current_texts = [current_texts[-1], row['sentence']]
                    current_group = row[['sent_year', 'sent_month', 'sent_day', 'sent_day_of_week']].to_dict()
                else:
                    # Append the sentence to current_texts
                    current_texts.append(row['sentence'])

                    # Update current_group
                    current_group = row[['sent_year', 'sent_month', 'sent_day', 'sent_day_of_week']].to_dict()

            # Append the last group to the result_dfs
            current_df = pd.DataFrame({'text': [" ".join(current_texts)], **current_group})
            result_dfs.append(current_df)

            # Concatenate all dataframes in the list
            result_df = pd.concat(result_dfs, ignore_index=True)

            # Remove emojis from text column
            result_df["text"] = result_df["text"].astype(str).apply(
                lambda x: x.encode('ascii', 'ignore').decode('ascii'))

            return result_df

        except Exception as e:
            raise Exception(f"Failed to concatenate text: {str(e)}")


class ProcessUserData:
    @staticmethod
    def parse_phone_number(raw_phone: str) -> dict:
        """
        Parses a phone number and returns the country code and the phone number without the country code.

        Args:
            raw_phone (str): The raw phone number to parse.

        Returns:
            dict: The region and country associated with the parsed phone number.

        Raises:
            TypeError: If phone_number is not of type str.
            ValueError: If a phone number cannot be parsed from the provided string.
        """
        validate_data_types(raw_phone, str, "phone_number")

        try:
            # Remove non-numeric characters and whitespace
            phone_number = "".join(filter(str.isdigit, raw_phone))

            # If it's a 10-digit number, assume it's from the US and add '+1'
            if len(phone_number) == 10:
                processed_number = "+1" + phone_number
            else:
                # Otherwise, add a '+' symbol to the front
                processed_number = "+" + phone_number

            # Attempt to parse the phone number
            parsed_number = phonenumbers.parse(processed_number, None)

            # Get region and country information
            region = geocoder.description_for_number(parsed_number, "en")
            country = geocoder.country_name_for_number(parsed_number, "en")

            return {"region": region, "country": country}

        except (ValueError, phonenumbers.NumberParseException) as e:
            raise ValueError(f"Failed to parse phone number: {str(e)}")

    @staticmethod
    def user_data_to_sentences(recipient_df: pd.DataFrame) -> List[str]:
        """
        Converts each row of the recipient DataFrame to a standardized sentence format.

        Args:
            recipient_df (pd.DataFrame): The recipient DataFrame with usernames and telephone numbers.

        Returns:
            List[str]: A list of standardized sentences.

        Raises:
            TypeError: If message_df or recipient_df are not pandas DataFrames, or thread_id is not an int.
            KeyError: If the specified columns do not exist in the DataFrames.
            Exception: If there's an error during processing.
        """
        # Validate input data types
        validate_dataframe(recipient_df)

        selected_cols = ["system_display_name", "profile_joined_name", "phone"]
        validate_columns_in_dataframe(recipient_df, selected_cols)

        try:
            user_df = recipient_df[selected_cols]

            processed_user_df = user_df.dropna().copy()

            processed_user_df["phone"] = (
                processed_user_df["phone"].astype(int).astype(str)
            )

            # Derive region and country values
            processed_user_df["phone_region"] = processed_user_df["phone"].apply(
                lambda x: ProcessUserData.parse_phone_number(x).get("region", None)
            )
            processed_user_df["phone_country"] = processed_user_df["phone"].apply(
                lambda x: ProcessUserData.parse_phone_number(x).get("country", None)
            )

            sentences = processed_user_df.apply(
                lambda row: f'{row["system_display_name"]} has a username of {row["profile_joined_name"]} '
                f'and a phone number of {row["phone"]}, which belongs to the region of {row["phone_region"]}, '
                f'in the country of {row["phone_country"]}.',
                axis=1,
            ).tolist()

            return sentences

        except Exception as e:
            raise Exception(
                f"Failed to produce sentences from user data for GenAI purposes: {str(e)}"
            )
