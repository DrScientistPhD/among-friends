# -*- coding: utf-8 -*-
import logging
from typing import List, Tuple

import click
import pandas as pd

# Local project imports
from src.data.data_mover import CSVMover, TextMover
from src.data.genai_preparation import ProcessMessageData, ProcessUserData
from src.data.recipient_mapper import RecipientMapper

# Instantiate logger
logger = logging.getLogger(__name__)
log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)


def import_data(data_source: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Imports necessary files and returns the dataframes.

    Args:
        data_source (str): Either 'production' or 'mocked' to specify the data source.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple of dataframes for messages and recipients.

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
    recipient = CSVMover.import_csv(data_dir, "recipient")

    return message, recipient


def export_message_user_txt_data(
    data_destination: str, message_data_txt: List[str], user_data_txt: List[str]
) -> None:
    """
    Export the message_data_txt and user_data_txt to either the prod or mock directory.

    Args:
        data_destination (str): Either 'production' or 'mocked' to specify the data source.
        message_data_txt (List[str]): The processed message data to save.
        user_data_txt (List[str]): The processed user data to save.

    Raises:
        ValueError: If a string other than "production" or "mocked" is provided.
    """

    if data_destination == "production":
        data_dir = "production_data/processed"
    elif data_destination == "mocked":
        data_dir = "mocked_data/processed"
    else:
        raise ValueError("Invalid selection. Use 'production' or 'mocked'.")

    logger.info(f"exporting message_data_txt to {data_destination} data directory")
    TextMover.export_sentences_to_file(message_data_txt, data_dir, "message_data")

    logger.info(f"exporting user_data_txt to {data_destination} data directory")
    TextMover.export_sentences_to_file(user_data_txt, data_dir, "user_data")


def create_processed_text(
    message: pd.DataFrame, recipient: pd.DataFrame, thread_id: int, default_author_name
) -> Tuple[List[str], List[str]]:
    """
    Processes the text and saves them as langchain Documents in a local directory.

    Args:
        message (pd.DataFrame): The raw message DataFrame.
        recipient (pd.DataFrame): The raw recipient DataFrame.
        thread_id (int): Thread ID to filter by.

    """
    logger.info("Instantiating RecipientMapper and ProcessMessageData")
    mapper = RecipientMapper(default_author_name=default_author_name)
    process_message_data = ProcessMessageData(recipient_mapper_instance=mapper)

    logger.info("Processing message data into discrete sentences.")
    processed_message_df = process_message_data.clean_up_messages(
        message, recipient, thread_id
    )

    message_sentences_df = ProcessMessageData.processed_message_data_to_sentences(
        processed_message_df
    )

    messages_data_txt = ProcessMessageData.concatenate_with_neighbors(
        message_sentences_df
    )

    logger.info("Processing user data into discrete sentences.")
    user_data_txt = ProcessUserData.user_data_to_sentences(recipient)

    return messages_data_txt, user_data_txt


@click.command()
@click.option(
    "--default-author-name",
    type=str,
    help="Specify how your username appears in the Signal app.",
)
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
def main(default_author_name, data_source, thread_id) -> None:
    """
    Runs the data preparation pipeline to process raw message and recipient data into text suitable for vectorization
    and Document creation.
    """
    try:
        logger.info("Starting data preparation pipeline for vectorization prep.")

        message, recipient = import_data(data_source)

        message_data_txt, user_data_txt = create_processed_text(
            message, recipient, thread_id, default_author_name
        )

        export_message_user_txt_data(data_source, message_data_txt, user_data_txt)

        logger.info("Data vectorization prep complete.")
    except Exception as e:
        logger.error(f"Error encountered: {str(e)}")


if __name__ == "__main__":
    main()
