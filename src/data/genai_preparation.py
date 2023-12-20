from src.data.csv_mover import CSVMover
from src.data.data_wrangling import DateTimeConverter
from src.data.recipient_mapper import RecipientMapper
import pandas as pd
import numpy as np
import json
from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)


class GenAiDataWrangler:
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

        return ' '.join(word for word in (text.split() or ['']) if len(word or '') <= 25) if text is not None else None

    @staticmethod
    def process_messages_for_genai(message_df: pd.DataFrame, recipient_df: pd.DataFrame, thread_id: int) -> (
            pd.DataFrame):
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
            message_df, ["date_sent", "from_recipient_id", "body", "quote_id", "quote_author", "quote_body"]
        )
        validate_columns_in_dataframe(
            recipient_df, ["profile_joined_name", "phone"]
        )

        # Check if there are any records that meet the specified thread_id filter
        if message_df[message_df["thread_id"] == thread_id].empty:
            raise ValueError(f"No records found for thread_id: {thread_id}")

        try:
            # Filter for the specified thread_id
            message_df = message_df[message_df["thread_id"] == thread_id].copy()

            # Convert date_sent column to datetime
            processed_data = DateTimeConverter.convert_unix_to_datetime(message_df, "date_sent")
            processed_data['date_sent_datetime'] = processed_data['date_sent_datetime'].astype(str)

            # Fill in NA values in quote_id column and convert column to datetime
            processed_data["quote_id"] = processed_data["quote_id"].fillna(0).astype(int)
            processed_data = DateTimeConverter.convert_unix_to_datetime(processed_data, "quote_id")
            processed_data["quote_id_datetime"] = processed_data["quote_id_datetime"].astype(str)
            processed_data.loc[processed_data['quote_id_datetime'] == "1970-01-01 00:00:00.000", "quote_id_datetime"] = ''

            # Fill in missing values for the quote_author column for easier data processing and select columns
            processed_data["quote_author"] = processed_data["quote_author"].fillna(0).astype(int).replace(0, '')

            # Select for appropriate columns and sort by date_sent_datetime
            message_columns = ["date_sent_datetime", "from_recipient_id", "body", "quote_id_datetime", "quote_author", "quote_body"]
            processed_data = processed_data[processed_data["body"].notnull()]

            selected_data = processed_data[message_columns].copy()

            # Sorting the DataFrame in place
            selected_data.sort_values(by='date_sent_datetime', ascending=True, inplace=True)

            # Replace recipient IDs with usernames
            recipient_id_to_name_dict = RecipientMapper.create_recipient_id_to_name_mapping(recipient_df)
            selected_data["from_recipient_id"] = selected_data["from_recipient_id"].map(recipient_id_to_name_dict)
            selected_data["quote_author"] = selected_data["quote_author"].map(recipient_id_to_name_dict)

            # Replace NaN values with None throughout the DataFrame and rename columns
            selected_data.replace(np.nan, '', inplace=True)
            selected_data.rename(columns={"from_recipient_id": "message_author"}, inplace=True)

            processed_message_df = selected_data.copy()

            # Removes unusually long text and empty body rows from the "body" and "quote_body" columns
            processed_message_df["body"] = processed_message_df["body"].apply(GenAiDataWrangler.filter_long_text)
            processed_message_df = processed_message_df[~processed_message_df["body"].str.strip().eq('')]

            processed_message_df["quote_body"] = processed_message_df["quote_body"].apply(GenAiDataWrangler.filter_long_text)

            return processed_message_df

        except Exception as e:
            raise Exception(f"Failed to process messages DataFrame for GenAI purposes: {str(e)}")

    def message_data_to_json(processed_message_df: pd.DataFrame) -> str:
        """
        Converts the processed message data to a JSON string and appends metadata information.

        Args:
            processed_message_df (pd.DataFrame): The processed message data

        Returns:
            str: A JSON string containing the message data and metadata.
        """
        # Validate input data types
        validate_dataframe(processed_message_df)
        validate_columns_in_dataframe(
            processed_message_df, ["date_sent_datetime", "message_author", "body", "quote_id_datetime", "quote_author", "quote_body"]
        )

        # Define the metadata
        message_metadata = {
            "fields_metadata": {
                "date_sent_datetime": {
                    "description": "Formatted datetime of when the message was sent",
                    "type": "string",
                    "source": "system"
                },
                "message_author": {
                    "description": "The name of the author who sent the chat message",
                    "type": "string",
                    "source": "system"
                },
                "body": {
                    "description": "The text contents of the chat message",
                    "type": "string",
                    "source": "user input"
                },
                "quote_id_datetime": {
                    "description": "Timestamp of the quoted chat message, if applicable",
                    "type": "string",
                    "source": "user input"
                },
                "quote_author": {
                    "description": "Author of the quoted message, if applicable",
                    "type": "string",
                    "source": "user input"
                },
                "quote_body": {
                    "description": "The text contents of the quoted message body, if applicable",
                    "type": "string",
                    "source": "user input"
                }
            }
        }

        # Convert the processed_message_df to a dictionary, and append the metadata to the beginning of said dictionary.
        message_data_dict = {**message_metadata, "messages": processed_message_df.to_dict(orient="records")}

        message_data_json = json.dumps(message_data_dict)

        return message_data_json



message = CSVMover.import_csv("data/production_data/raw", "message")
recipient = CSVMover.import_csv("data/production_data/raw", "recipient")

message_data = GenAiDataWrangler.process_messages_for_genai(message, recipient, 2)

message_data_json = GenAiDataWrangler.message_data_to_json(message_data)






























selected_cols = ["profile_joined_name", "phone"]
new_df = recipient[selected_cols]

new_df = new_df.dropna().copy()

new_df["phone"] = new_df["phone"].astype(str)



user_metadata = {
    "fields_metadata": {
        "profile_joined_name": {
            "description": "The user's profile name",
            "type": "string",
            "source": "user"
        },
        "phone": {
            "description": "The phone number associated with the user's account, listed using the North American "
                           "Numbering Plan.",
            "type": "string",
            "source": "system"
        }
    }
}






recipient_id_to_name = RecipientMapper.create_recipient_id_to_name_mapping(recipient)

def remove_nan_values_from_dict(dictionary):
    return {k: v for k, v in dictionary.items() if pd.notna(v)}

recipient_dict = remove_nan_values_from_dict(recipient_id_to_name)


# Update the following function so that it renames the column that is being mapped
def replace_values_in_column(df, column_name, dictionary):
    df[column_name] = df[column_name].map(dictionary)
    return df

mapped_author_df = replace_values_in_column(slim_message_df, "from_recipient_id", recipient_dict)
mapped_author_df = replace_values_in_column(mapped_author_df, "quote_author", recipient_dict)
mapped_author_df.rename(columns={"from_recipient_id": "from_author", "quote_id": "quote_date"}, inplace=True)

mapped_author_df = mapped_author_df.where(pd.notnull(mapped_author_df), None)
