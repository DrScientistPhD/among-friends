import os
from unittest.mock import MagicMock, patch

import pytest

from src.data.manage_vectordb import ManageVectorDb


class TestManageVectorDb:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        os.environ["PINECONE_API_KEY"] = "dummy_api_key"
        os.environ["PINECONE_ENV"] = "dummy_env"
        self.mock_pinecone_index = MagicMock()
        self.mock_pinecone_index.describe_index_stats.return_value.total_vector_count = (
            100
        )
        self.mock_pinecone_index.query.return_value = {
            "matches": [{"id": str(i)} for i in range(10)]
        }
        self.mock_pinecone_index.upsert = MagicMock()

        with patch("pinecone.Index", return_value=self.mock_pinecone_index):
            with patch("pinecone.init"):
                yield

    @staticmethod
    def create_document_mock(content="dummy_content", metadata=None):
        metadata = metadata or {}
        return MagicMock(page_content=content, metadata=metadata)

    def test_initialize_pinecone(self):
        with patch("pinecone.init") as mock_init:
            ManageVectorDb.initialize_pinecone()
            mock_init.assert_called_once_with(
                api_key="dummy_api_key", environment="dummy_env"
            )

    def test_index_already_exists(self, capsys):
        index_name = "test_index"

        # Simulate that the index already exists
        with patch("pinecone.list_indexes") as mock_list_indexes:
            mock_list_indexes.return_value = [index_name]

            # Test whether create_index prints the expected message when the index already exists
            ManageVectorDb.create_index(index_name)

        # Capture the printed output
        captured = capsys.readouterr()

        # Check if the print statement occurred
        assert f"Index {index_name} already exists.\n" == captured.out

    def test_get_all_ids_from_index_raises_exception(self):
        index_name = "test_index"

        # Simulate an error when retrieving IDs from the index
        with patch("pinecone.Index") as mock_index:
            # Create a mock instance of Index and set a side_effect for query to raise an error
            mock_query = mock_index.return_value.query
            mock_query.side_effect = Exception("Error retrieving IDs")

            # Test if get_all_ids_from_index raises an Exception on error
            with pytest.raises(Exception) as exc_info:
                ManageVectorDb.get_all_ids_from_index(index_name)

            # Check if the error message contains the expected substring
            assert "Failed to retrieve IDs from index:" in str(exc_info.value)

    def test_upsert_text_embeddings_to_pinecone_raises_exception(self):
        index_name = "test_index"
        docs = [
            self.create_document_mock(content=f"dummy_content_{i}") for i in range(10)
        ]

        # Mocking get_all_ids_from_index to return an empty set, simulating no existing IDs
        with patch.object(ManageVectorDb, "get_all_ids_from_index", return_value=set()):
            with patch("pinecone.Index") as mock_index:
                mock_upsert = mock_index.return_value.upsert
                mock_upsert.side_effect = Exception("Error upserting embeddings")

                with pytest.raises(Exception) as exc_info:
                    ManageVectorDb.upsert_text_embeddings_to_pinecone(index_name, docs)

                assert "Failed to upsert embeddings to index:" in str(exc_info.value)
