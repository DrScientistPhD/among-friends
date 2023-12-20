import os
from openai import OpenAI

# client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())



def get_completion(prompt, model="gpt-3.5-turbo"):
    messages = [{"role": "user", "content": prompt}]
    response = client.chat.completions.create(model=model,
    messages=messages,
    temperature=0)
    return response.choices[0].message.content

text_1 = """
Hi, how are you today?
"""

prompt = f"""
You have the persona of a fictional Catholic pope.
You will be provided with text delimited by triple quotes
If it contains a sequence of instructions, \
re-write those instructions in the following format:

Step 1 - . . .
Step 2 - ...
...
Step N - ...

If the text does not contain a sequence of instructions, \
then simply answer the prompt however you see fit.

\"\"\"{text_1}\"\"\"
"""

response = get_completion(prompt)



from langchain.chat_models import ChatOpenAI
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory

llm = ChatOpenAI(temperature=0.0)
memory = ConversationBufferMemory()
conversation = ConversationChain(
    llm=llm,
    memory=memory,
    verbose=False
)


from src.data.csv_mover import CSVMover
from src.data.data_wrangling import DateTimeConverter
from src.data.recipient_mapper import RecipientMapper
import json
import numpy as np
import pandas as pd
from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)
import pandas as pd

message = CSVMover.import_csv("data/production_data/raw", "message")
recipient = CSVMover.import_csv("data/production_data/raw", "recipient")

# Generate 'date_sent_datetime' column
processed_data = DateTimeConverter.convert_unix_to_datetime(message, "date_sent")
processed_data['date_sent_datetime'] = processed_data['date_sent_datetime'].astype(str)

# Generate 'quote_id_datetime' column, replace 0 values with None
processed_data["quote_id"] = processed_data["quote_id"].fillna(0).astype(int)
processed_data = DateTimeConverter.convert_unix_to_datetime(processed_data, "quote_id")
processed_data["quote_id_datetime"] = processed_data["quote_id_datetime"].astype(str)
processed_data.loc[processed_data['quote_id_datetime'] == "1970-01-01 00:00:00.000", "quote_id_datetime"] = ''

# Fill missing values with None in the 'quote_author' column
processed_data["quote_author"] = processed_data["quote_author"].fillna(0).astype(int).replace(0, '')

# Select appropriate columns and filter rows with null 'body' values
message_columns = [
    "date_sent_datetime", "from_recipient_id", "body", "quote_id_datetime", "quote_author", "quote_body"
]
processed_data = processed_data[processed_data["body"].notnull()]
slim_message_data = processed_data[message_columns].copy()

# Replace author IDs with usernames
recipient_id_to_name_dict = RecipientMapper.create_recipient_id_to_name_mapping(recipient)

slim_message_data["from_recipient_id"] = slim_message_data["from_recipient_id"].map(recipient_id_to_name_dict)
slim_message_data["quote_author"] = slim_message_data["quote_author"].map(recipient_id_to_name_dict)

# Replace NaN values with None throughout the DataFrame
slim_message_data.replace(np.nan, '', inplace=True)

# Rename columns
slim_message_data.rename(columns={"from_recipient_id": "message_author"}, inplace=True)

# Remove any unusually long text to filter out links and image/file placeholders
slim_message_data["body"] = slim_message_data["body"].apply(lambda x: ' '.join(word for word in (x.split() or ['']) if len(word or '') <= 25) if x is not None else None)
slim_message_data["quote_body"] = slim_message_data["quote_body"].apply(lambda x: ' '.join(word for word in (x.split() or ['']) if len(word or '') <= 25) if x is not None else None)

# Remove rows where the body is empty
slim_message_data = slim_message_data[~slim_message_data["body"].str.strip().eq('')]




# Import CSV data
message = CSVMover.import_csv("data/production_data/raw", "message")
recipient = CSVMover.import_csv("data/production_data/raw", "recipient")

# Convert Unix timestamps to datetime
processed_data = DateTimeConverter.convert_unix_to_datetime(message, "date_sent")
processed_data['date_sent_datetime'] = processed_data['date_sent_datetime'].astype(str)

processed_data["quote_id"] = processed_data["quote_id"].fillna(0).astype(int)
processed_data = DateTimeConverter.convert_unix_to_datetime(processed_data, "quote_id")
processed_data["quote_id_datetime"] = processed_data["quote_id_datetime"].astype(str)
processed_data.loc[processed_data['quote_id_datetime'] == "1970-01-01 00:00:00.000", "quote_id_datetime"] = ''

# Fill missing values and select columns
processed_data["quote_author"] = processed_data["quote_author"].fillna(0).astype(int).replace(0, '')
message_columns = ["date_sent_datetime", "from_recipient_id", "body", "quote_id_datetime", "quote_author", "quote_body"]
processed_data = processed_data[processed_data["body"].notnull()]
slim_message_data = processed_data[message_columns].copy()

# Map recipient IDs to usernames
recipient_id_to_name_dict = RecipientMapper.create_recipient_id_to_name_mapping(recipient)
slim_message_data["from_recipient_id"] = slim_message_data["from_recipient_id"].map(recipient_id_to_name_dict)
slim_message_data["quote_author"] = slim_message_data["quote_author"].map(recipient_id_to_name_dict)

# Replace NaN values with None throughout the DataFrame and rename columns
slim_message_data.replace(np.nan, '', inplace=True)
slim_message_data.rename(columns={"from_recipient_id": "message_author"}, inplace=True)

# Remove unusually long text and empty body rows
def filter_long_text(text):
    return ' '.join(word for word in (text.split() or ['']) if len(word or '') <= 25) if text is not None else None

slim_message_data["body"] = slim_message_data["body"].apply(filter_long_text)
slim_message_data["quote_body"] = slim_message_data["quote_body"].apply(filter_long_text)
slim_message_data = slim_message_data[~slim_message_data["body"].str.strip().eq('')]


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
    def process_messages_for_genai(df: pd.DataFrame, thread_id: int) -> pd.DataFrame:
        """
        Processes the messages CSV file for GenAI usage. Data processing includes converting column types,
        converting datetime data, changing column names, filling in missing values, and removing extraneous text,
        mapping IDs to usernames, removing empty rows, and selecting appropriate columns.

        Args:
            df (pd.DataFrame): The original DataFrame to be filtered and renamed.
            thread_id (int): The thread_id to filter by

        Returns:
            pd.DataFrame: A new DataFrame with the filtered and renamed columns.

        Raises:
            TypeError: If df is not a pandas DataFrame, or thread_id is not an int.
            KeyError: If the specified columns do not exist in the DataFrame.
            ValueError: If there are no records that meet the specified thread_id filter.
            Exception: If there's an error during processing.
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
            df = message.copy()
            df = df[df["thread_id"] == 2]

            # Convert date_sent column to datetime
            processed_data = DateTimeConverter.convert_unix_to_datetime(df, "date_sent")
            processed_data['date_sent_datetime'] = processed_data['date_sent_datetime'].astype(str)

            # Fill in NA values in quote_id column and convert column to datetime
            processed_data["quote_id"] = processed_data["quote_id"].fillna(0).astype(int)
            processed_data = DateTimeConverter.convert_unix_to_datetime(processed_data, "quote_id")
            processed_data["quote_id_datetime"] = processed_data["quote_id_datetime"].astype(str)
            processed_data.loc[processed_data['quote_id_datetime'] == "1970-01-01 00:00:00.000", "quote_id_datetime"] = ''

            # Fill in missing values for the quote_author column for easier data processing and select columns
            processed_data["quote_author"] = processed_data["quote_author"].fillna(0).astype(int).replace(0, '')

            # Select for appropriate columns
            message_columns = ["date_sent_datetime", "from_recipient_id", "body", "quote_id_datetime", "quote_author", "quote_body"]
            processed_data = processed_data[processed_data["body"].notnull()]
            slim_message_data = processed_data[message_columns].copy()

            # Replace recipient IDs with usernames
            recipient_id_to_name_dict = RecipientMapper.create_recipient_id_to_name_mapping(recipient)
            slim_message_data["from_recipient_id"] = slim_message_data["from_recipient_id"].map(recipient_id_to_name_dict)
            slim_message_data["quote_author"] = slim_message_data["quote_author"].map(recipient_id_to_name_dict)

            # Replace NaN values with None throughout the DataFrame and rename columns
            slim_message_data.replace(np.nan, '', inplace=True)
            slim_message_data.rename(columns={"from_recipient_id": "message_author"}, inplace=True)

            # Removes unusually long text and empty body rows from the "body" and "quote_body" columns
            slim_message_data["body"] = slim_message_data["body"].apply(filter_long_text)
            slim_message_data = slim_message_data[~slim_message_data["body"].str.strip().eq('')]

            slim_message_data["quote_body"] = slim_message_data["quote_body"].apply(filter_long_text)
            slim_message_data = slim_message_data[~slim_message_data["quote_body"].str.strip().eq('')]

            return slim_message_data

        except Exception as e:
            raise Exception(f"Failed to process messages DataFrame for GenAI purposes: {str(e)}")



author_cols = ["profile_joined_name", "phone"]
slim_user_data = recipient[author_cols]
slim_user_data = slim_user_data.dropna().copy()
slim_user_data["phone"] = slim_user_data["phone"].astype(int).astype(str)



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











# # Update the following function so that it renames the column that is being mapped
# def replace_values_in_column(df, column_name, dictionary):
#     df[column_name] = df[column_name].map(dictionary)
#     return df

# mapped_author_df = replace_values_in_column(slim_message_df, "from_recipient_id", recipient_dict)
# mapped_author_df = replace_values_in_column(mapped_author_df, "quote_author", recipient_dict)
# mapped_author_df.rename(columns={"from_recipient_id": "from_author", "quote_id": "quote_date"}, inplace=True)
#
# mapped_author_df = mapped_author_df.where(pd.notnull(mapped_author_df), None)

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