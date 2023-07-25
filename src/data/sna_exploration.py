import emoji
import numpy as np
import pandas as pd

# First, let's read in the necessary CSV files as Pandas DataFrames
df_message = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")
df_recipient = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/recipient.csv"
)
df_reaction = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/reaction.csv"
)

# Create a dictionary to map recipient_id to system_given_name in df_recipient
recipient_id_to_name = df_recipient.set_index("_id")["profile_joined_name"].to_dict()

# Update the 'author_name' and 'quote_recipient_name' columns in df_message to use profile_joined_name
df_message["author_name"] = df_message["from_recipient_id"].map(recipient_id_to_name)
df_message["quote_recipient_name"] = df_message["quote_author"].map(
    recipient_id_to_name
)

# Map 'author_id' in df_reaction to get the author's full name
df_reaction["author_name"] = df_reaction["author_id"].map(recipient_id_to_name)

# Merge df_message and df_reaction on 'message_id' to associate reactions with messages
df_message_reaction = pd.merge(
    df_message,
    df_reaction,
    left_on="_id",
    right_on="message_id",
    suffixes=("", "_reaction"),
)

# Display the DataFrame with 'author_name', 'quote_recipient_name', 'emoji', and 'author_name_reaction' columns
df_message_reaction[
    ["author_name", "quote_recipient_name", "emoji", "author_name_reaction"]
].head()

# Select 'author_name_reaction' as 'from', 'author_name' as 'to', 'date_sent' as 'message_timestamp', and 'date_sent_reaction' as 'reaction_timestamp' columns from df_message_reaction
df_social_network = df_message_reaction[
    ["author_name_reaction", "author_name", "date_sent", "date_sent_reaction"]
].copy()

df_social_network.columns = ["from", "to", "message_timestamp", "reaction_timestamp"]

# Add 'body' as 'message_text' and 'emoji' as 'reaction_text' columns to df_social_network
df_social_network["message_text"] = df_message_reaction["body"]
df_social_network["reaction_text"] = df_message_reaction["emoji"]

df_social_network.head()


def translate_emoji(text):
    return " ".join(emoji.demojize(emo).strip(":") for emo in text)


df_social_network["reaction_text_translation"] = df_social_network[
    "reaction_text"
].apply(translate_emoji)

# Calculate the time difference in seconds
df_social_network["time_diff"] = (
    df_social_network["reaction_timestamp"] - df_social_network["message_timestamp"]
) / 1000


import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

# Assuming your dataframe is called 'df' and the reaction time column is 'reaction_time'
x = df_social_network.index.values
y = df_social_network["time_diff"].values

df_social_network[["reaction_timestamp", "message_timestamp"]]

df_social_network.to_csv("/Users/raymondpasek/Downloads/foobar.csv", index=False)

df_social_network["time_diff"].describe()

# Use the 75th percentile as the half life constant. Arbitrarily chosen as this will ensure most people who respond
# within a few minutes get near full credit, and then
half_life = df_social_network["time_diff"].quantile(0.75)

# Calculate the decay constant
decay_constant = np.log(2) / half_life


reaction_base_value = 0.5
# Calculate the weight using the exponential decay function (0.5 is arbitrary max value of reactions, will adjust later)
df_social_network['weight'] = reaction_base_value * np.exp(-decay_constant * df_social_network['time_diff'])

df_social_network.loc[df_social_network['weight'] < (reaction_base_value * 0.1), 'weight'] = (reaction_base_value * 0.1)


df_social_network.head()






# weights of quote texts
df_message = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")
df_recipient = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/recipient.csv"
)

# Create a dictionary to map recipient_id to profile_joined_name in df_recipient
recipient_id_to_name = df_recipient.set_index("_id")["profile_joined_name"].to_dict()

# Perform a self-join on the 'df_message' DataFrame where 'date_sent' matches 'quote_id' in another row
df_quotes = df_message.merge(df_message, left_on='date_sent', right_on='quote_id', suffixes=('_quote', '_original'))

# Rename the columns for clarity
df_quotes.rename(columns={
    'from_recipient_id_original': 'from',
    'from_recipient_id_quote': 'to',
    'date_sent_original': 'response_timestamp',
    'date_sent_quote': 'quoted_message_timestamp',
    'body_original': 'response_text',
    'body_quote': 'quoted_text',
}, inplace=True)

# Map recipient ids to names
df_quotes['from'] = df_quotes['from'].map(recipient_id_to_name)
df_quotes['to'] = df_quotes['to'].map(recipient_id_to_name)

# Select only the required columns
df_quotes = df_quotes[['from', 'response_timestamp', 'response_text', 'to', 'quoted_message_timestamp', 'quoted_text']]

# Calculate the time difference between the response and the quoted message
df_quotes['time_diff'] = (df_quotes['response_timestamp'] - df_quotes['quoted_message_timestamp']) / 1000

df_quotes.head()






# Use the 75th percentile as the half life constant. Arbitrarily chosen as this will ensure most people who respond
# within a few minutes get near full credit, and then
half_life = df_quotes["time_diff"].quantile(0.75)

# Calculate the decay constant
decay_constant = np.log(2) / half_life

quote_base_value = 2
# Calculate the weight using the exponential decay function (2 is arbitrary max value of reactions, will adjust later)
df_quotes['weight'] = quote_base_value * np.exp(-decay_constant * df_quotes['time_diff'])

df_quotes.loc[df_quotes['weight'] < (quote_base_value * 0.1), 'weight'] = (quote_base_value * 0.1)

df_quotes.head()








message_df = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")
recipient_df = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/recipient.csv"
)

df = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message_small.csv")

import polars as pl
from polars import col
import pandas as pd
import datetime

def load_and_preprocess_data_polars(file_path):
    # Load the data
    df = pl.read_csv(file_path)

    # # Convert the 'date_sent' column to datetime
    # df = df.with_columns(
    #     pl.col("date_sent").apply(lambda x: datetime.datetime.fromtimestamp(x / 1000), return_dtype=pl.Datetime).alias(
    #         "date_sent"))

    # Create two copies of the dataframe for comments and responses
    df_comments = df.clone()
    df_responses = df.clone()

    # Rename the columns to match the requested format
    df_comments = df_comments.rename({
        '_id': 'comment_id',
        'date_sent': 'comment_date_sent',
        'from_recipient_id': 'from_recipient_id',
        'body': 'comment_body'
    })

    df_responses = df_responses.rename({
        '_id': 'response_id',
        'date_sent': 'response_date_sent',
        'from_recipient_id': 'from_recipient_id',
        'body': 'response_body'
    })

    # Sort 'df_comments' and 'df_responses' by 'date_sent'
    df_comments = df_comments.sort('comment_date_sent')
    df_responses = df_responses.sort('response_date_sent')

    return df_comments, df_responses


def perform_self_join_polars(df_comments, df_responses):


    #####  COMMENT AND RESPONSE COLUMNS ARE REVSERSED!!! FIX THIS!

    # Perform the self join operation where a response is strictly after a comment and within 10 minutes
    df_self_join = df_comments.join(
        df_responses,
        on=['from_recipient_id'],
        how='left'
    )

    df_self_join = df_self_join.with_columns([
        pl.when(col('response_date_sent').is_not_null() &
                (col('response_date_sent').cast(pl.Int64) - col('comment_date_sent').cast(pl.Int64) <= 600))
        .then(1).otherwise(0).alias('flag')
    ])

    # Filter out rows where a chat participant responded to themselves
    df_self_join = df_self_join.filter(col('comment_id') != col('response_id'))

    # Filter out rows where a comment doesn't have a response
    df_self_join = df_self_join.filter(col('response_body').is_not_null())

    # Filter the dataframe to only include the rows where flag = 1
    df_self_join = df_self_join.filter(col('flag') == 1)

    df_self_join = df_self_join.with_columns((df_self_join['comment_date_sent'] - df_self_join['response_date_sent']).alias('time_diff'))

    df_self_join = df_self_join.with_columns((df_self_join['time_diff'] / 1000).alias('time_diff'))

    return df_self_join


df_comments, df_responses = load_and_preprocess_data_polars("/Users/raymondpasek/Repos/among-friends/data/raw/message_small.csv")
df_self_join = perform_self_join_polars(df_comments, df_responses)
df_self_join.head(10)


foo = df_self_join.slice(0,20).to_pandas()




import polars as pl

# Assuming you have a Polars DataFrame named 'df' with the i64 column to be divided by 1000
df = pl.DataFrame({
    'column_to_divide': [2000, 5000, 10000, 8000]
})

# Divide the i64 column by 1000 and create a new column 'result'
df = df.with_columns((df['column_to_divide'] / 1000).alias('result'))

# Display the resulting DataFrame
print(df)






type(df_self_join)

def get_size(bytes, suffix="B"):
    # Scale bytes to its proper format
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f}{unit}{suffix}"
        bytes /= factor

# Assuming `df` is your Polars dataframe
import sys

# Before conversion
polars_size = sys.getsizeof(df_self_join)
polars_size_friendly = get_size(polars_size)

# Convert to pandas
df_pandas = df_self_join.to_pandas()

# After conversion
pandas_size = sys.getsizeof(df_pandas)
pandas_size_friendly = get_size(pandas_size)

print(f"Size of Polars DataFrame: {polars_size_friendly}")
print(f"Size of Pandas DataFrame: {pandas_size_friendly}")