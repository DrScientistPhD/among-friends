import emoji
import numpy as np
import pandas as pd

from src.data.csv_importer import CSVImporter
from src.data.data_wrangling import (DateTimeConverter, MessageDataWrangler,
                                     QuotationResponseDataWrangler,
                                     EmojiDataWrangler)
from src.data.sna_preparation import SnaDataWrangler

from src.data.time_calculations import TimeCalculations


# Import message.csv and reaction.csv
message_df = CSVImporter.import_csv("message")
emoji_df = CSVImporter.import_csv("reaction")

# Filter and rename columns in message_df
message_slim_df = MessageDataWrangler.filter_and_rename_messages_df(message_df, thread_id=2)




# Concatenate comment threads to pair comments and responses, and sets the number of rows to look forward for each
# individual comment to the total number of unique comment_from_recipient_ids.
group_n = message_slim_df["comment_from_recipient_id"].nunique()
comments_responses_df = MessageDataWrangler.concatenate_comment_threads(message_slim_df, group_n)

# Quantify the time difference between comments and responses
response_time_df = TimeCalculations.calculate_time_diff(comments_responses_df, "response_date_sent", "comment_date_sent")

# Calculate the decay constant based on the 75 percentile of the time_diff column
response_decay_constant = TimeCalculations.calculate_decay_constant(response_time_df, "time_diff")

# Using a base value of 1, derive the weight of each comment-response interaction
response_weight_df = TimeCalculations.calculate_weight(response_time_df, response_decay_constant, 1.0)

# Create more readable datetime columns
response_weight_df = DateTimeConverter.convert_unix_to_datetime(response_weight_df, "comment_date_sent")
response_weight_df = DateTimeConverter.convert_unix_to_datetime(response_weight_df, "response_date_sent")

# Subset the response_weight_df and add an interaction category for each record
response_weight_slim_df = SnaDataWrangler.standardize_response_react_dataframe(response_weight_df)



# Filter and rename columns in emoji_df
emoji_slim_df = EmojiDataWrangler.filter_and_rename_emojis_df(emoji_df)

# Merge messages to emojis
comments_emojis_df = EmojiDataWrangler.merge_message_with_emoji(message_slim_df, emoji_slim_df)

# Quantify the time difference between comments and emojis
emojis_time_df = TimeCalculations.calculate_time_diff(comments_emojis_df, "emoji_date_sent", "comment_date_sent")

# Calculate the decay constant based on the 75 percentile of the time_diff column
emoji_decay_constant = TimeCalculations.calculate_decay_constant(emojis_time_df, "time_diff")

# Using a base value of 1.5, derive the weight of each comment-response interaction
emoji_weight_df = TimeCalculations.calculate_weight(emojis_time_df, emoji_decay_constant, 1.5)

# Create more readable datetime columns
emoji_weight_df = DateTimeConverter.convert_unix_to_datetime(emoji_weight_df, "comment_date_sent")
emoji_weight_df = DateTimeConverter.convert_unix_to_datetime(emoji_weight_df, "emoji_date_sent")

# Subset the emoji_weight_df and add an interaction category for each record
emoji_weight_slim_df = SnaDataWrangler.standardize_emoji_react_dataframe(emoji_weight_df)



# Create a dataframe of quotations and their responses
quotation_response_df = QuotationResponseDataWrangler.create_quotation_response_df(message_slim_df)

# Quantify the time difference between comments and reactions
quotation_time_df = TimeCalculations.calculate_time_diff(quotation_response_df, "response_date_sent", "quotation_date_sent")

# Calculate the decay constant based on the 75 percentile of the time_diff column
quotation_decay_constant = TimeCalculations.calculate_decay_constant(quotation_time_df, "time_diff")

# Using a base value of 2.0, derive the weight of each quotation-response interaction
quotation_weight_df = TimeCalculations.calculate_weight(quotation_time_df, quotation_decay_constant, 2.0)

# Create more readable datetime columns
quotation_weight_df = DateTimeConverter.convert_unix_to_datetime(quotation_weight_df, "quotation_date_sent")
quotation_weight_df = DateTimeConverter.convert_unix_to_datetime(quotation_weight_df, "response_date_sent")

# Subset the response_weight_df and add an interaction category for each record
quotation_weight_slim_df = SnaDataWrangler.create_reacted_dataframe(quotation_weight_df, quotation_cols, "quotation")