
from src.data.csv_mover import CSVMover
from src.data.data_wrangling import DateTimeConverter
from src.data.recipient_mapper import RecipientMapper
import pandas as pd
import json
from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)

message = CSVMover.import_csv("data/production_data/raw", "message")
recipient = CSVMover.import_csv("data/production_data/raw", "recipient")

columns_to_filter = ["date_sent", "date_sent_datetime", "from_recipient_id", "body", "quote_id", "quote_author", "quote_body"]

processed_message_df = DateTimeConverter.convert_unix_to_datetime(message, "date_sent")
processed_message_df["quote_id"] = processed_message_df["quote_id"].fillna(0).astype(int)
processed_message_df["quote_author"] = processed_message_df["quote_author"].fillna(0).astype(int)
processed_message_df = DateTimeConverter.convert_unix_to_datetime(message, "quote_id")

processed_message_df['date_sent_datetime'] = processed_message_df['date_sent_datetime'].astype(str)
processed_message_df['quote_id'] = processed_message_df['quote_id'].astype(str)

slim_message_df = processed_message_df[columns_to_filter].copy()


def filter_and_rename_genai_messages_df(df: pd.DataFrame, thread_id: int) -> pd.DataFrame:
    """
    Filters and renames columns of a given DataFrame representing messages for GenAI processing.

    Args:
        df (pd.DataFrame): The original DataFrame to be filtered and renamed.
        thread_id (int): The thread_id to filter by

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
        df, ["date_sent", "from_recipient_id", "body", "quote_id", "quote_author", "quote_body"]
    )

    # Check if there are any records that meet the specified thread_id filter
    if df[df["thread_id"] == thread_id].empty:
        raise ValueError(f"No records found for thread_id: {thread_id}")

    try:
        df = df[df["thread_id"] == thread_id]

    except Exception as e:
        raise Exception(f"Failed to filter and rename messages DataFrame for GenAI purposes: {str(e)}")






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

def convert_df_to_json(df):
    return df.to_dict(orient="records")

mapped_author_json = convert_df_to_json(mapped_author_df)

message_metadata = {
    "fields_metadata": {
        "date_sent_datetime": {
            "description": "Formatted datetime of when the message was sent",
            "type": "string",
            "source": "system"
        },
        "from_author": {
            "description": "The name of the author who sent the chat message",
            "type": "string",
            "source": "system"
        },
        "body": {
            "description": "The text contents of the chat message",
            "type": "string",
            "source": "user input"
        },
        "quote_date": {
            "description": "Timestamp of the quoted chat message, if applicable",
            "type": "string",
            "source": "user input"
        },
        "quote_author": {
            "description": "Author of quoted message, if applicable",
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

data = {**message_metadata, "messages": mapped_author_json}

mapped_author_json_string = json.dumps(mapped_author_json)