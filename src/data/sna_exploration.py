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
recipient_id_to_name = df_recipient.set_index("_id")["system_display_name"].to_dict()

# Update the 'author_name' and 'quote_recipient_name' columns in df_message to use system_display_name
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

# Calculate the weight using the exponential decay function
df_social_network['weight'] = 0.5 * np.exp(-decay_constant * df_social_network['time_diff'])

df_social_network.head()



# weights of quote texts
df_message = pd.read_csv("/Users/raymondpasek/Repos/among-friends/data/raw/message.csv")
df_recipient = pd.read_csv(
    "/Users/raymondpasek/Repos/among-friends/data/raw/recipient.csv"
)

# Create a dictionary to map recipient_id to system_display_name in df_recipient
recipient_id_to_name = df_recipient.set_index("_id")["system_display_name"].to_dict()

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
df_quotes['time_diff'] = df_quotes['response_timestamp'] - df_quotes['quoted_message_timestamp']

df_quotes.head()
