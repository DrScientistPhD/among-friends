import emoji
import numpy as np
import pandas as pd

from src.data.csv_importer import CSVImporter
from src.data.data_wrangling import (DateTimeConverter, MessageDataWrangler,
                                     QuotationResponseDataWrangler,
                                     ReactionDataWrangler)

from src.data.recipient_mapper import RecipientMapper
from src.data.time_calculations import TimeCalculations


# Import message.csv and reaction.csv
message_df = CSVImporter.import_csv("message")
reaction_df = CSVImporter.import_csv("reaction")

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



# Filter and rename columns in reaction_df
reaction_slim_df = ReactionDataWrangler.filter_and_rename_reactions_df(reaction_df)

# Merge messages to reactions
comments_reactions_df = ReactionDataWrangler.merge_message_with_reaction(message_slim_df, reaction_slim_df)

# Quantify the time difference between comments and reactions
reactions_time_df = TimeCalculations.calculate_time_diff(comments_reactions_df, "reaction_date_sent", "comment_date_sent")

# Calculate the decay constant based on the 75 percentile of the time_diff column
reaction_decay_constant = TimeCalculations.calculate_decay_constant(reactions_time_df, "time_diff")

# Using a base value of 1.5, derive the weight of each comment-response interaction
reaction_weight_df = TimeCalculations.calculate_weight(reactions_time_df, reaction_decay_constant, 1.5)

# Create more readable datetime columns
reaction_weight_df = DateTimeConverter.convert_unix_to_datetime(reaction_weight_df, "comment_date_sent")
reaction_weight_df = DateTimeConverter.convert_unix_to_datetime(reaction_weight_df, "reaction_date_sent")



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