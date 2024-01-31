import hashlib
import os
import time
from typing import List

import numpy as np
import pinecone
from dotenv import find_dotenv, load_dotenv
from sentence_transformers import SentenceTransformer

from src.data.data_validation import validate_data_types


_ = load_dotenv(find_dotenv())


class ManageVectorDb:
    _pinecone_initialized = False

    @classmethod
    def initialize_pinecone(cls):
        if not cls._pinecone_initialized:
            try:
                cls.pinecone_api_key = os.environ["PINECONE_API_KEY"]
                cls.pinecone_env = os.environ["PINECONE_ENV"]
                pinecone.init(
                    api_key=cls.pinecone_api_key, environment=cls.pinecone_env
                )
                cls._pinecone_initialized = True
            except Exception:
                raise Exception(
                    f"Failed to initialize Pinecone. Check env file to ensure that the Pinecone API key and "
                    f"environment are set correctly."
                )

    @staticmethod
    def create_index(index_name: str) -> None:
        """
        Creates a new index in the Pinecone vector database.

        Args:
            index_name (str): The name of the index to be created.

        Raises:
            TypeError: If index_name is not of the expected type (str).
            Warning: If the pinecone index already exists.
            Exception: If there's an error during the index creation process.
        """
        # Validate input data
        validate_data_types(index_name, str, "index_name")

        try:
            ManageVectorDb.initialize_pinecone()

            if index_name in pinecone.list_indexes():
                print(f"Index {index_name} already exists.")

            else:
                pinecone.create_index(
                    name=index_name,
                    metric="cosine",
                    shards=1,
                    dimension=768
                    # 768 is the dimension of the all-distilroberta-v1 embeddings
                )

                # Wait for index to be initialized
                while not pinecone.describe_index(index_name).status["ready"]:
                    time.sleep(1)

        except Exception as e:
            raise Exception(f"Failed to create index: {str(e)}")

    @staticmethod
    def get_all_ids_from_index(index_name: str) -> set:
        """
        Retrieves all IDs from a Pinecone vector database index.

        Args:
            index_name: The name of the index to get ids from.

        Returns:
            set: A set containing all retrieved IDs.

        Raises:
            TypeError: If index_name is not a string.
            Exception: If there's an error during the ID retrieval process.
        """
        # Validate input data
        validate_data_types(index_name, str, "index_name")

        try:
            ManageVectorDb.initialize_pinecone()

            index = pinecone.Index(index_name=index_name)

            # Get the number of vectors in the index
            num_vectors = index.describe_index_stats().total_vector_count
            all_ids = set()

            # Need to query indexes in batches of 10000. Overkill for this project, but good practice.
            while len(all_ids) < num_vectors:
                # Need to generate a random input vector for the query (hacky workaround because Pinecone doesn't
                # have a better way)
                input_vector = np.random.rand(768).tolist()
                results = index.query(
                    top_k=10000,
                    include_values=False,
                    include_metadata=False,
                    vector=input_vector,
                )
                ids = {result.id for result in results["matches"]}
                all_ids |= ids  # Merge the sets

            return all_ids

        except Exception as e:
            raise Exception(f"Failed to retrieve IDs from index: {str(e)}")

    @staticmethod
    def upsert_text_embeddings_to_pinecone(index_name: str, docs: List) -> None:
        """
        Upserts text embeddings to a Pinecone vector database index.

        Args:
            index_name (str): The name of the index to upsert embeddings to.
            docs (List): A list of Document objects containing the page content and metadata.

        Raises:
            TypeError: If index_name is not a string or docs is not a list.
            Exception: If there's an error during the upsert process.
        """

        # Validate input data
        validate_data_types(index_name, str, "index_name")
        validate_data_types(docs, List, "docs")

        try:
            # Initialize pinecone and get the index object
            ManageVectorDb.initialize_pinecone()

            index = pinecone.Index(index_name=index_name)

            # Get all existing IDs in the index
            existing_ids = ManageVectorDb.get_all_ids_from_index(index_name)

            model = SentenceTransformer("all-distilroberta-v1")
            embeddings_to_upsert = []

            # Check and upsert unique embeddings and metadata into the Pinecone index for this batch
            for i, doc in enumerate(docs):
                page_content = doc.page_content
                metadata = doc.metadata
                metadata["text"] = page_content
                # Hash the content to create IDs, that way we can avoid upserting the same content twice.
                content_hash = hashlib.sha256(page_content.encode()).hexdigest()

                if content_hash not in existing_ids:
                    content_embedding = model.encode([page_content])[0]
                    embeddings_to_upsert.append(
                        (content_hash, content_embedding.tolist(), metadata)
                    )

                # Upsert in batches of 500
                if len(embeddings_to_upsert) >= 500 or i == len(docs) - 1:
                    embeddings_to_upsert_formatted = [
                        (id_, emb, meta) for id_, emb, meta in embeddings_to_upsert
                    ]
                    if embeddings_to_upsert_formatted:
                        index.upsert(embeddings_to_upsert_formatted)
                    embeddings_to_upsert = []

        except Exception as e:
            raise Exception(f"Failed to upsert embeddings to index: {str(e)}")
