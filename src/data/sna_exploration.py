from src.data.csv_importer import CSVImporter
from src.data.data_wrangling import EmojiDataWrangler, MessageDataWrangler
from src.data.sna_preparation import SnaDataWrangler
from src.models.sna_graph_builder import SnaGraphBuilder, SnaMetricCalculator

# Import message.csv and reaction.csv
message_df = CSVImporter.import_csv("message")
emoji_df = CSVImporter.import_csv("reaction")

# Filter and rename columns in message_df
message_slim_df = MessageDataWrangler.filter_and_rename_messages_df(
    message_df, thread_id=2
)
emoji_slim_df = EmojiDataWrangler.filter_and_rename_emojis_df(emoji_df)

# Quantify the weights of all interactions
response_weight_slim_df = SnaDataWrangler.process_data_for_sna(
    "response", 1.0, message_slim_df
)
emoji_weight_slim_df = SnaDataWrangler.process_data_for_sna(
    "emoji", 1.5, message_slim_df, emoji_slim_df
)
quotation_weight_slim_df = SnaDataWrangler.process_data_for_sna(
    "quotation", 2.0, message_slim_df
)

# Vertically concatenate all interactions dataframes
nodes_edges_df = SnaDataWrangler.concatenate_dataframes_vertically(
    [response_weight_slim_df, emoji_weight_slim_df, quotation_weight_slim_df]
)

# Create Network Graph
network_graph = SnaGraphBuilder.create_network_graph(nodes_edges_df)

# Calculate In-Eigenvector centrality and closeness metrics
eigenvector_closeness_metrics = (
    SnaMetricCalculator.generate_eigenvector_closeness_metrics(network_graph)
)


