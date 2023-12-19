# -*- coding: utf-8 -*-
import logging
from typing import Tuple

import click
import pandas as pd

# Local project imports
from src.data.csv_mover import CSVMover
from src.data.data_wrangling import EmojiDataWrangler, MessageDataWrangler
from src.data.recipient_mapper import RecipientMapper
from src.data.sna_preparation import SnaDataWrangler

# Instantiate logger
logger = logging.getLogger(__name__)
log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)


def import_data(data_source: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Imports necessary files and returns the dataframes.

    Args:
        data_source (str): Either 'production' or 'mocked' to specify the data source.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple of dataframes for messages, emojis, and recipients.

    Raises:
        ValueError: If a string other than "production" or "mocked" is provided.
    """
    logger.info(f"Importing necessary files from {data_source}_data")

    if data_source == "production":
        data_dir = "data/production_data/raw"
    elif data_source == "mocked":
        data_dir = "data/mocked_data/raw"
    else:
        raise ValueError("Invalid data_source. Use 'production' or 'mocked'.")

    message = CSVMover.import_csv(data_dir, "message")
    emoji = CSVMover.import_csv(data_dir, "reaction")
    recipient = CSVMover.import_csv(data_dir, "recipient")

    return message, emoji, recipient


def export_nodes_edges_data(data_destination: str, df: pd.DataFrame) -> None:
    """
    Export the nodes_edges_df to either the prod or mock directory.

    Args:
        data_destination (str): Either 'production' or 'mocked' to specify the data source.
        df (pd.DataFrame): The processed nodes-edges dataframe to save.

    Raises:
        ValueError: If a string other than "production" or "mocked" is provided.
    """
    logger.info(f"exporting nodes_edges_df to {data_destination} data directory")

    if data_destination == "production":
        data_dir = "production_data/processed"
    elif data_destination == "mocked":
        data_dir = "mocked_data/processed"
    else:
        raise ValueError("Invalid selection. Use 'production' or 'mocked'.")

    CSVMover.export_csv(df, data_dir, "nodes_edges_df")


def generate_edge_weights(
    message_df: pd.DataFrame, emoji_df: pd.DataFrame, thread_id: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Generates weight dataframes for response, emoji, and quotation edges.

    Args:
        message_df (pd.DataFrame): DataFrame containing message data.
        emoji_df (pd.DataFrame): DataFrame containing emoji data.
        thread_id (int): Thread ID to filter by.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Tuple of dataframes for response, emoji, and quotation weights.
    """
    logger.info("Generating response, emoji, and quotation edges")
    message_slim = MessageDataWrangler.filter_and_rename_sna_messages_df(
        message_df, thread_id=thread_id
    )

    emoji_slim = EmojiDataWrangler.filter_and_rename_emojis_df(emoji_df)

    response_weight_slim = SnaDataWrangler.process_data_for_sna(
        "response", 1.0, message_slim
    )
    emoji_weight_slim = SnaDataWrangler.process_data_for_sna(
        "emoji", 1.5, message_slim, emoji_slim
    )
    quotation_weight_slim = SnaDataWrangler.process_data_for_sna(
        "quotation", 2.0, message_slim
    )

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
    nodes_edges_raw_id = SnaDataWrangler.concatenate_dataframes_vertically(
        [response, emoji, quotation]
    )
    return nodes_edges_raw_id


@click.command()
@click.option(
    "--data-source",
    type=click.Choice(["production", "mocked"]),
    default="mocked",
    show_default=True,
    help="Specify the data source to use. Choices are 'production' or 'mocked'.",
)
@click.option(
    "--thread-id",
    type=int,
    default=2,
    show_default=True,
    help="Specify the thread ID to filter by.",
)
def main(data_source, thread_id) -> None:
    """Runs data processing scripts to turn data from either production_data
    or mocked_data into cleaned data ready to be analyzed (saved in the
    specified output file).
    """
    try:
        logger.info("Making final network data set from raw data")

        message_df, emoji_df, recipient_df = import_data(data_source)
        (
            response_weight_slim_df,
            emoji_weight_slim_df,
            quotation_weight_slim_df,
        ) = generate_edge_weights(message_df, emoji_df, thread_id)

        nodes_edges_raw_id_df = create_final_dataframe(
            response_weight_slim_df, emoji_weight_slim_df, quotation_weight_slim_df
        )

        logger.info("Mapping names to participant IDs")
        nodes_edges_df = RecipientMapper.update_node_participant_names(
            nodes_edges_raw_id_df, recipient_df
        )

        logger.info("Saving final nodes-edges dataframe")
        export_nodes_edges_data(data_source, nodes_edges_df)

    except ValueError as e:
        logger.error(f"Error encountered: {str(e)}")


if __name__ == "__main__":
    main()
