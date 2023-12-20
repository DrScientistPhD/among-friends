# -*- coding: utf-8 -*-
import logging
from typing import Tuple

import click
import pandas as pd

# Local project imports
from src.data.data_mover import CSVMover, JSONMover
from src.data.genai_preparation import GenAiDataWrangler

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
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple of dataframes for messages and recipients.

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


def generate_message_and_user_data(
    message: pd.DataFrame, recipient: pd.DataFrame, thread_id: int
) -> Tuple[str, str]:
    """
    Generates the JSON objects for messages and users.

    Args:
        message (pd.DataFrame): The raw message DataFrame.
        recipient (pd.DataFrame): The raw recipient DataFrame.
        thread_id (int): Thread ID to filter by.

    Returns:
        Tuple[str, str]: A tuple of JSON strings for messages and users.
    """
    logger.info("Generating JSON objects for messages and users")
    processed_message_df = GenAiDataWrangler.process_messages_for_genai(
        message_df=message, recipient_df=recipient, thread_id=thread_id
    )

    message_data_json = GenAiDataWrangler.message_data_to_json(processed_message_df)

    processed_user_df = GenAiDataWrangler.process_user_data_for_genai(
        recipient_df=recipient
    )

    user_data_json = GenAiDataWrangler.user_data_to_json(processed_user_df)

    return message_data_json, user_data_json


def export_message_and_user_data(
    data_destination: str, message_data_json: str, user_data_json: str
) -> None:
    """
    Saves the message and user JSON data to separate files to the prod or mock directory.

    Args:
        data_destination (str): Either 'production' or 'mocked' to specify the data source.
        message_data_json (str): JSON string to be saved to the file.
        user_data_json (str): JSON string to be saved to the file.

    Raises:
        ValueError: If a string other than "production" or "mocked" is provided.
    """
    logger.info(f"Exporting GenAI json objects to {data_destination} data directory")

    if data_destination == "production":
        data_dir = "production_data/processed"
    elif data_destination == "mocked":
        data_dir = "mocked_data/processed"
    else:
        raise ValueError("Invalid selection. Use 'production' or 'mocked'.")

    JSONMover.export_json(message_data_json, data_dir, "message_data")
    JSONMover.export_json(user_data_json, data_dir, "user_data")


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
        logger.info("Generating and saving data for GenAI...")

        message, recipient = import_data(data_source)

        message_data_json, user_data_json = generate_message_and_user_data(
            message, recipient, thread_id
        )

        export_message_and_user_data(data_source, message_data_json, user_data_json)

        logger.info("Done generating and saving data for GenAI.")

    except ValueError as e:
        logger.error(f"Error encountered: {str(e)}")


if __name__ == "__main__":
    main()
