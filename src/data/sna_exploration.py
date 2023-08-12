import emoji
import numpy as np
import pandas as pd

from src.data.csv_importer import CSVImporter
from src.data.data_wrangling import (DateTimeConverter, MessageDataWrangler,
                                     QuotationResponseDataWrangler,
                                     ReactionDataWrangler)
from src.data.emoji_translation import EmojiTranslator
from src.data.recipient_mapper import RecipientMapper
from src.data.time_calculations import TimeCalculations


# Import message.csv and reaction.csv
message_df = CSVImporter.import_csv("message")
reaction_df = CSVImporter.import_csv("reaction")

# Filter and rename columns in message_df
message_slim_df = MessageDataWrangler.filter_and_rename_messages_df(message_df)

# Concatenate comment threads to pair comments and responses, and sets the number of rows to look forward for each
# individual comment to the total number of unique comment_from_recipient_ids.
group_n = message_slim_df["comment_from_recipient_id"].nunique()
comments_responses_df = MessageDataWrangler.concatenate_comment_threads(message_slim_df, group_n)

# Quantify the time difference between comments and responses
# TODO: Figure out why some values are negative.
response_time_df = TimeCalculations.calculate_time_diff(comments_responses_df, "response_date_sent", "comment_date_sent")

# Calculate the decay constant based on the 75 percentile of the time_diff column
response_decay_constant = TimeCalculations.calculate_decay_constant(response_time_df, "time_diff")

# Using a base value of 1, derive the weight of each comment-response interaction
response_weight_df = TimeCalculations.calculate_weight(response_time_df, response_decay_constant, 1.0)

# Create more readable datetime columns
response_weight_df = DateTimeConverter.convert_unix_to_datetime(response_weight_df, "comment_date_sent")
response_weight_df = DateTimeConverter.convert_unix_to_datetime(response_weight_df, "response_date_sent")


# Filter and rename columns in reaction_df
reaction_slim_df = ReactionDataWrangler.filter_and_rename_reactions_df(reaction_df)


# TODO: Turn the merge into a function for data_wrangling
# Merge df_message and df_reaction on 'message_id' to associate reactions with messages
df_message_reaction = pd.merge(
    message_slim_df,
    reaction_slim_df,
    left_on="comment_id",
    right_on="message_id",
    suffixes=("", "_reaction"),
)


# First, let's read in the necessary CSV files as Pandas DataFrames
df_message = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")
df_recipient = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/recipient.csv"
)
df_reaction = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/reaction.csv"
)

# # Create a dictionary to map recipient_id to system_given_name in df_recipient
# recipient_id_to_name = df_recipient.set_index("_id")["profile_joined_name"].to_dict()
#
# # Update the 'author_name' and 'quote_recipient_name' columns in df_message to use profile_joined_name
# df_message["author_name"] = df_message["from_recipient_id"].map(recipient_id_to_name)
# df_message["quote_recipient_name"] = df_message["quote_author"].map(
#     recipient_id_to_name
# )

# # Map 'author_id' in df_reaction to get the author's full name
# df_reaction["author_name"] = df_reaction["author_id"].map(recipient_id_to_name)

raw_df = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")

columns_to_filter = [
    "_id",
    "thread_id",
    "form_recipient_id",
    "quote_id",
    "date_sent",
    "body",
]

renamed_df = raw_df.rename(
    columns={
        "_id": "comment_id",
        "thread_id": "comment_thread_id",
        "from_recipient_id": "comment_recipient_id",
        "date_sent": "comment_date_sent",
        "body": "comment_body",
    }
)

select_df = renamed_df[columns_to_filter].copy()


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
df_social_network["weight"] = reaction_base_value * np.exp(
    -decay_constant * df_social_network["time_diff"]
)

df_social_network.loc[
    df_social_network["weight"] < (reaction_base_value * 0.1), "weight"
] = (reaction_base_value * 0.1)


df_social_network.head()


# weights of quote texts
df_message = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")
df_recipient = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/recipient.csv"
)

# Create a dictionary to map recipient_id to profile_joined_name in df_recipient
recipient_id_to_name = df_recipient.set_index("_id")["profile_joined_name"].to_dict()


message_df = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")

quotation_df = message_df.rename(
    columns={
        "_id": "quotation_id",
        "date_sent": "quotation_date_sent",
        "thread_id": "quotation_thread_id",
        "from_recipient_id": "quotation_from_recipient_id",
        "body": "quotation_body",
    }
)

quotation_columns_to_filter = [
    "quotation_id",
    "quotation_date_sent",
    "quotation_thread_id",
    "quotation_from_recipient_id",
    "quotation_body",
]

quotation_slim_df = quotation_df[quotation_columns_to_filter].copy()

response_df = df_message.copy().rename(
    columns={
        "_id": "response_id",
        "date_sent": "response_date_sent",
        "thread_id": "response_thread_id",
        "from_recipient_id": "response_from_recipient_id",
        "body": "response_body",
    }
)

response_columns_to_filer = [
    "response_id",
    "response_date_sent",
    "response_thread_id",
    "response_from_recipient_id",
    "response_body",
    "quote_id",
]

response_slim_df = response_df[response_columns_to_filer].copy()


quotation_response_df = quotation_slim_df.merge(
    response_slim_df, left_on="quotation_date_sent", right_on="quote_id"
)


# Calculate the time difference between the response and the quoted message
quotation_response_df["time_diff"] = (
    quotation_response_df["response_date_sent"]
    - quotation_response_df["quotation_date_sent"]
) / 1000


# Perform a self-join on the 'df_message' DataFrame where 'date_sent' matches 'quote_id' in another row
df_quotes = df_message.merge(
    df_message,
    left_on="date_sent",
    right_on="quote_id",
    suffixes=("_quote", "_original"),
)

# Rename the columns for clarity
df_quotes.rename(
    columns={
        "from_recipient_id_original": "from",
        "from_recipient_id_quote": "to",
        "date_sent_original": "response_timestamp",
        "date_sent_quote": "quoted_message_timestamp",
        "body_original": "response_text",
        "body_quote": "quoted_text",
    },
    inplace=True,
)

# Map recipient ids to names
df_quotes["from"] = df_quotes["from"].map(recipient_id_to_name)
df_quotes["to"] = df_quotes["to"].map(recipient_id_to_name)

# Select only the required columns
df_quotes = df_quotes[
    [
        "from",
        "response_timestamp",
        "response_text",
        "to",
        "quoted_message_timestamp",
        "quoted_text",
    ]
]

# Calculate the time difference between the response and the quoted message
df_quotes["time_diff"] = (
    df_quotes["response_timestamp"] - df_quotes["quoted_message_timestamp"]
) / 1000

df_quotes.head()


# Use the 75th percentile as the half life constant. Arbitrarily chosen as this will ensure most people who respond
# within a few minutes get near full credit, and then
half_life = df_quotes["time_diff"].quantile(0.75)

# Calculate the decay constant
decay_constant = np.log(2) / half_life

quote_base_value = 2
# Calculate the weight using the exponential decay function (2 is arbitrary max value of reactions, will adjust later)
df_quotes["weight"] = quote_base_value * np.exp(
    -decay_constant * df_quotes["time_diff"]
)

df_quotes.loc[df_quotes["weight"] < (quote_base_value * 0.1), "weight"] = (
    quote_base_value * 0.1
)

df_quotes.head()


import numpy as np
import pandas as pd
import polars as pl
from polars.functions import col

df_group_membership = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/group_membership.csv"
)
df_groups = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/groups.csv")

# Merge the two dataframes on the group_id column
df_merged = pd.merge(df_group_membership, df_groups, on="group_id")

# Filter the dataframe to include only the group "BBBF"
df_filtered = df_merged[df_merged["title"] == "BBBF"]

# Calculate the number of distinct individuals in the group
group_participants_n = df_filtered["recipient_id_x"].nunique()


df_large = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")
# Filter out records where the body is missing text
df_large = df_large[df_large["body"].notna()]

# Filter out records where quote_author is not 0
df_large = df_large[df_large["quote_author"] == 0]

# Select the columns that are found in message_small.csv
df_large = df_large[["_id", "date_sent", "from_recipient_id", "body"]]

# Convert the pandas dataframe to a polars dataframe
df_polars = pl.DataFrame(df_large)

# Sort the dataframe by the date_sent column
df_polars = df_polars.sort("date_sent")

# Rename the columns in the original dataframe
df_polars.columns = [
    "comment_id",
    "comment_date_sent",
    "comment_from_recipient_id",
    "comment_body",
]

# Create a list to store the new dataframes
dfs = []

# Iterate over the rows of the dataframe
for i in range(len(df_polars)):
    # Get the next rows
    next_rows = df_polars.slice(i + 1, len(df_polars) - i - 1)

    # Filter the rows where comment_from_recipient_id is not equal to response_from_recipient_id
    next_rows = next_rows.filter(
        col("comment_from_recipient_id") != df_polars[i]["comment_from_recipient_id"]
    )

    # Get the first n number of rows for each comment, where n is equal the number of group_participants
    next_rows = next_rows.slice(0, min(group_participants_n, len(next_rows)))

    for j in range(len(next_rows)):
        # Rename the columns of the next row before concatenation
        next_row = next_rows.slice(j, 1)
        next_row.columns = [
            "response_id",
            "response_date_sent",
            "response_from_recipient_id",
            "response_body",
        ]

        # Stack the current row and the next row horizontally
        new_df = pl.concat([df_polars.slice(i, 1), next_row], how="horizontal")

        # Append the new dataframe to the list
        dfs.append(new_df)

# Concatenate all the dataframes vertically to get the final dataframe
df_final_polars = pl.concat(dfs, how="vertical")

# Subtract the comment_date_sent value from the response_date_sent value, convert to seconds, and assign a new column name
df_final_polars = df_final_polars.with_columns(
    (pl.col("response_date_sent") - pl.col("comment_date_sent")).alias("time_diff")
    / 1000
)

# Convert the final Polars dataframe to a pandas dataframe and get the first 100 rows
df_final_pandas = df_final_polars.to_pandas()


# Use the 75th percentile as the half life constant. Arbitrarily chosen as this will ensure most people who respond
# within a few minutes get near full credit, and then
half_life = df_final_pandas["time_diff"].quantile(0.75)

# Calculate the decay constant
decay_constant = np.log(2) / half_life

response_base_value = 1
# Calculate the weight using the exponential decay function (1 is arbitrary max value of reactions, will adjust later)
df_final_pandas["weight"] = response_base_value * np.exp(
    -decay_constant * df_final_pandas["time_diff"]
)


df_final_pandas.head()


# Calculate the time difference between the response and the quoted message
quotation_response_df["time_diff"] = (
    quotation_response_df["response_date_sent"]
    - quotation_response_df["quotation_date_sent"]
) / 1000

# Calculate the time difference between the response and the quoted message
df_quotes["time_diff"] = (
    df_quotes["response_timestamp"] - df_quotes["quoted_message_timestamp"]
) / 1000


# Calculate the time difference in seconds
df_social_network["time_diff"] = (
    df_social_network["reaction_timestamp"] - df_social_network["message_timestamp"]
) / 1000
