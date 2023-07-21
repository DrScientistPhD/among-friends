import pandas as pd
import emoji
import numpy as np

# First, let's read in the necessary CSV files as Pandas DataFrames
df_message = pd.read_csv('/Users/raymondpasek/Repos/among-friends/data/raw/message.csv')
df_recipient = pd.read_csv('/Users/raymondpasek/Repos/among-friends/data/raw/recipient.csv')
df_reaction = pd.read_csv('/Users/raymondpasek/Repos/among-friends/data/raw/reaction.csv')

# Create a dictionary to map recipient_id to system_given_name in df_recipient
recipient_id_to_name = df_recipient.set_index('_id')['system_display_name'].to_dict()

# Update the 'author_name' and 'quote_recipient_name' columns in df_message to use system_display_name
df_message['author_name'] = df_message['from_recipient_id'].map(recipient_id_to_name)
df_message['quote_recipient_name'] = df_message['quote_author'].map(recipient_id_to_name)

# Map 'author_id' in df_reaction to get the author's full name
df_reaction['author_name'] = df_reaction['author_id'].map(recipient_id_to_name)

# Merge df_message and df_reaction on 'message_id' to associate reactions with messages
df_message_reaction = pd.merge(df_message, df_reaction, left_on='_id', right_on='message_id', suffixes=('', '_reaction'))

# Display the DataFrame with 'author_name', 'quote_recipient_name', 'emoji', and 'author_name_reaction' columns
df_message_reaction[['author_name', 'quote_recipient_name', 'emoji', 'author_name_reaction']].head()

# Select 'author_name_reaction' as 'from', 'author_name' as 'to', 'date_sent' as 'message_timestamp', and 'date_sent_reaction' as 'reaction_timestamp' columns from df_message_reaction
df_social_network = df_message_reaction[['author_name_reaction', 'author_name', 'date_sent', 'date_received']].copy()
df_social_network.columns = ['from', 'to', 'message_timestamp', 'reaction_timestamp']

# Add 'body' as 'message_text' and 'emoji' as 'reaction_text' columns to df_social_network
df_social_network['message_text'] = df_message_reaction['body']
df_social_network['reaction_text'] = df_message_reaction['emoji']

df_social_network.head()

def translate_emoji(text):
    return ' '.join(emoji.demojize(emo).strip(':') for emo in text)

df_social_network['reaction_text_translation'] = df_social_network['reaction_text'].apply(translate_emoji)

# Calculate the time difference in seconds
df_social_network['time_diff'] = (df_social_network['reaction_timestamp'] - df_social_network['message_timestamp']) / 1000



from scipy.optimize import curve_fit
import matplotlib.pyplot as plt

# Assuming your dataframe is called 'df' and the reaction time column is 'reaction_time'
x = df_social_network.index.values
y = df_social_network['time_diff'].values

# Define the exponential decay function
def exponential_decay(x, A, decay_rate):
    return A * np.exp(-decay_rate * x)

# Fit the exponential decay model to the data with bounds
bounds = ([-np.inf, 0], [np.inf, np.inf])  # Lower and upper bounds for [A, decay_rate]
popt, _ = curve_fit(exponential_decay, x, y, bounds=bounds)

# Extract the decay rate from the fitted parameters
decay_rate = popt[1]

# Calculate half-life
half_life = np.log(2) / decay_rate

# Visualize the data and fitted model
plt.plot(x, y, label='Data')
plt.plot(x, exponential_decay(x, *popt), 'r', label='Fitted Model')
plt.xlabel('Event Index or Time')
plt.ylabel('Reaction Time')
plt.legend()

# Display the plot
plt.show()

print(f"Estimated Half-Life: {half_life}")


# Define the maximum weight and the half-life
max_weight = 0.5
half_life = 60  # for example, 60 seconds

# Calculate the decay constant
lambda_ = np.log(2) / half_life

# Calculate the weight using the exponential decay function
df_social_network['weight'] = max_weight * np.exp(-lambda_ * df_social_network['time_diff'])


df_social_network[["reaction_timestamp", "message_timestamp"]]