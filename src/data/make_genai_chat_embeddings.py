# -*- coding: utf-8 -*-
import logging
from typing import List, Tuple

import click
from langchain.docstore.document import Document

# Local project imports
from src.data.data_mover import DocumentMover
from src.data.manage_vectordb import ManageVectorDb

# Instantiate logger
logger = logging.getLogger(__name__)
log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)


def import_data(data_source: str) -> Tuple[List[Document], List[Document]]:
    """
    Imports necessary files and returns the langchain Documents as lists.

    Args:
        data_source (str): Either 'production' or 'mocked' to specify the data source.

    Returns:
        Tuple[List[Document], List[Document]]: A tuple of dataframes for messages and recipients.

    Raises:
        ValueError: If a string other than "production" or "mocked" is provided.
    """
    logger.info(f"Importing necessary files from {data_source}_data")

    if data_source == "production":
        data_dir = "data/production_data/processed"
    elif data_source == "mocked":
        data_dir = "data/mocked_data/processed"
    else:
        raise ValueError("Invalid data_source. Use 'production' or 'mocked'.")

    messages_docs = DocumentMover.load_message_text(
        data_dir, "message_data"
    )

    user_docs = DocumentMover.load_and_split_user_text(
        data_dir, "user_data"
    )

    return messages_docs, user_docs


@click.command()
@click.option(
    "--data-source",
    type=click.Choice(["production", "mocked"]),
    default="mocked",
    show_default=True,
    help="Specify the data source to use. Choices are 'production' or 'mocked'.",
)
def main(data_source) -> None:
    """
    Runs the data processing scripts to generate and upload text embeddings to pinecone from processed message and user
    data.
    """
    try:
        logger.info("Generating text embeddings for message and user data.")

        messages_docs, user_docs = import_data(data_source)

        logger.info("Creating among-friends index in Pinecone.")
        index_name = "among-friends"
        ManageVectorDb.create_index(index_name)

        logger.info("Generating and upserting user text embeddings.")
        ManageVectorDb.upsert_text_embeddings_to_pinecone(index_name, user_docs)

        logger.info("Generating and upserting message text embeddings.")
        ManageVectorDb.upsert_text_embeddings_to_pinecone(index_name, messages_docs)

        logger.info("New text embeddings generated and upserted successfully.")
    except Exception as e:
        logger.error(f"Error encountered: {str(e)}")


if __name__ == "__main__":
    main()
