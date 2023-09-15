# -*- coding: utf-8 -*-
import logging
from typing import Tuple

# Local project imports
from src.data.csv_mover import CSVMover
from src.data.data_wrangling import EmojiDataWrangler, MessageDataWrangler
from src.data.sna_preparation import SnaDataWrangler
from src.data.recipient_mapper import RecipientMapper
import pandas as pd

# Instantiate logger
logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """
    Configures the logging format and level for the script.
    """
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)


def import_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Imports necessary files and returns the dataframes.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple of dataframes for messages, emojis, and recipients.
    """
    logger.info("Importing necessary files")
    message = CSVMover.import_csv("raw", "message")
    emoji = CSVMover.import_csv("raw", "reaction")
    recipient = CSVMover.import_csv("raw", "recipient")
    return message, emoji, recipient


def generate_edge_weights(
    message_df: pd.DataFrame, emoji_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Generates weight dataframes for response, emoji, and quotation edges.

    Args:
        message_df (pd.DataFrame): DataFrame containing message data.
        emoji_df (pd.DataFrame): DataFrame containing emoji data.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Tuple of dataframes for response, emoji, and quotation weights.
    """
    logger.info("Generating response, emoji, and quotation edges")
    message_slim = MessageDataWrangler.filter_and_rename_messages_df(message_df, thread_id=2)
    emoji_slim = EmojiDataWrangler.filter_and_rename_emojis_df(emoji_df)

    response_weight_slim = SnaDataWrangler.process_data_for_sna("response", 1.0, message_slim)
    emoji_weight_slim = SnaDataWrangler.process_data_for_sna("emoji", 1.5, message_slim, emoji_slim)
    quotation_weight_slim = SnaDataWrangler.process_data_for_sna("quotation", 2.0, message_slim)

    return response_weight_slim, emoji_weight_slim, quotation_weight_slim


def create_final_dataframe(
    response: pd.DataFrame, emoji: pd.DataFrame, quotation: pd.DataFrame
) -> pd.DataFrame:
    """
    Concatenates the given dataframes vertically to create the final dataframe.

    Args:
        response (pd.DataFrame): DataFrame containing response weight data.
        emoji (pd.DataFrame): DataFrame containing emoji weight data.
        quotation (pd.DataFrame): DataFrame containing quotation weight data.

    Returns:
        pd.DataFrame: Concatenated dataframe.
    """
    logger.info("Generating nodes-edges raw dataframe")
    nodes_edges_raw_id = SnaDataWrangler.concatenate_dataframes_vertically([response, emoji, quotation])
    return nodes_edges_raw_id


def main() -> None:
    """
    Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready for network analysis (saved in ../processed).
    """
    logger.info("Making final network data set from raw data")

    message_df, emoji_df, recipient_df = import_data()
    response_weight_slim_df, emoji_weight_slim_df, quotation_weight_slim_df = generate_edge_weights(message_df, emoji_df)

    nodes_edges_raw_id_df = create_final_dataframe(response_weight_slim_df, emoji_weight_slim_df, quotation_weight_slim_df)

    logger.info("Mapping names to participant IDs")
    nodes_edges_df = RecipientMapper.update_node_participant_names(nodes_edges_raw_id_df, recipient_df)

    logger.info("Saving final nodes-edges dataframe")
    CSVMover.export_csv(nodes_edges_df, "nodes_edges_df")


if __name__ == "__main__":
    configure_logging()
    main()
