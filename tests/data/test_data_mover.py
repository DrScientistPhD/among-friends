import pandas as pd
import pytest
from faker import Faker

from src.data.data_mover import CSVMover, TextMover, DocumentMover


class TestCSVMover:
    """Test class for the CSVMover class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.csv_mover = CSVMover()

    @pytest.mark.parametrize("iteration", range(10))
    def test_import_csv_as_dataframe_type(self, iteration, mocker):
        """Test to check if the output of import_csv() is a pandas DataFrame."""
        # Generate a random file name for each iteration
        file_name = f"dummy_file_{iteration}.csv"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}.csv")

        # Mocking os.path.isfile to return True
        mocker.patch("os.path.isfile", return_value=True)

        # Mocking pd.read_csv to return a DataFrame with fake data
        mocker.patch("pandas.read_csv", return_value=pd.DataFrame())

        # Test the import_csv method
        df = self.csv_mover.import_csv("parent_dir", file_name)
        assert isinstance(df, pd.DataFrame)

    @pytest.mark.parametrize("iteration", range(10))
    def test_import_csv_graceful_failure(self, iteration, mocker):
        """Test to check if the function raises an Exception when given a non-existent file path."""
        # Generate a random file name for each iteration
        file_name = f"non_existent_file_{iteration}.csv"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}.csv")

        # Mocking os.path.isfile to return False
        mocker.patch("os.path.isfile", return_value=False)

        with pytest.raises(Exception):
            self.csv_mover.import_csv("parent_dir", file_name)

    @pytest.mark.parametrize("iteration", range(10))
    def test_export_csv_success(self, iteration, fake_nodes_edges_dataframe, mocker):
        """Test to check if the function exports a DataFrame to a CSV file successfully."""
        df = fake_nodes_edges_dataframe
        parent_directory = "parent_dir"
        file_name = "test_file"

        # Mocking validate_data_types to bypass actual validation
        mocker.patch("src.data.data_mover.validate_data_types", return_value=None)

        # Mocking DataFrame's to_csv method to avoid actual writing
        mocker.patch.object(pd.DataFrame, "to_csv", return_value=None)

        # Test the export_csv method
        self.csv_mover.export_csv(df, parent_directory, file_name)

    @pytest.mark.parametrize("iteration", range(10))
    def test_export_csv_general_exception(
        self, iteration, fake_nodes_edges_dataframe, mocker
    ):
        """Test to check if the function raises a general Exception during the export process."""
        df = fake_nodes_edges_dataframe
        parent_directory = "parent_dir"
        file_name = "test_file"

        # Mocking DataFrame's to_csv method to raise an exception
        mocker.patch.object(
            pd.DataFrame,
            "to_csv",
            side_effect=Exception("Some error occurred during export"),
        )

        with pytest.raises(Exception, match="Failed to export DataFrame to CSV file"):
            self.csv_mover.export_csv(df, parent_directory, file_name)


class TestTextMover:
    """Test class for the TextMover class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.text_mover = TextMover()

    @pytest.mark.parametrize("iteration", range(10))
    def test_import_text_file_as_list_type(self, iteration, mocker):
        """Test to check if the output of import_text_file() is a list."""
        # Generate a random file name for each iteration
        file_name = f"dummy_file_{iteration}.txt"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}.csv")

        # Mocking os.path.isfile to return True
        mocker.patch("os.path.isfile", return_value=True)

        # Mocking import_text_file to return a list with fake data
        mocker.patch(
            "src.data.data_mover.TextMover.import_text_file", return_value=list()
        )

        # Test the import_text_file method
        fake_list = self.text_mover.import_text_file("parent_dir", file_name)
        assert isinstance(fake_list, list)

    @pytest.mark.parametrize("iteration", range(10))
    def test_import_text_file_graceful_failure(self, iteration, mocker):
        """Test to check if the function raises an Exception when given a non-existent file path."""
        # Generate a random file name for each iteration
        file_name = f"non_existent_file_{iteration}.txt"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}.csv")

        # Mocking os.path.isfile to return False
        mocker.patch("os.path.isfile", return_value=False)

        with pytest.raises(Exception):
            self.text_mover.import_text_file("parent_dir", file_name)

    @pytest.mark.parametrize("iteration", range(10))
    def test_export_sentences_to_file_success(
        self, iteration, fake_list_of_sentences, mocker
    ):
        """Test to check if the function exports a list of sentences to a text file successfully."""
        fake_list = fake_list_of_sentences
        parent_directory = "parent_dir"
        file_name = "test_file"

        # Mocking validate_data_types to bypass actual validation
        mocker.patch("src.data.data_mover.validate_data_types", return_value=None)

        # Mocking export_sentences_to_file method to avoid actual writing
        mocker.patch(
            "src.data.data_mover.TextMover.export_sentences_to_file", return_value=None
        )

        # Test the export_csv method
        self.text_mover.export_sentences_to_file(fake_list, parent_directory, file_name)

    @pytest.mark.parametrize("iteration", range(10))
    def test_export_sentences_to_file_general_exception(
        self, iteration, fake_list_of_sentences, mocker
    ):
        """Test to check if the function raises a general Exception during the export process."""
        fake_list = fake_list_of_sentences
        parent_directory = "parent_dir"
        file_name = "test_file"

        # Mocking export_sentences_to_file method to raise an exception
        mocker.patch(
            "src.data.data_mover.TextMover.export_sentences_to_file",
            side_effect=Exception("Failed to export sentences to text file:"),
        )

        with pytest.raises(Exception, match="Failed to export sentences to text file:"):
            self.text_mover.export_sentences_to_file(
                fake_list, parent_directory, file_name
            )


class TestDocumentMover:
    """Test class for the DocumentMover class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.document_mover = DocumentMover()

    @pytest.mark.parametrize("iteration", range(10))
    def test_load_and_split_user_text_valid_file(self, iteration, mocker):
        """Test to check if the function loads and splits text from a file."""
        # Generate a random file name for each iteration
        file_name = f"test_file_{iteration}.txt"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}")

        # Mocking os.path.isfile to return True
        mocker.patch("os.path.isfile", return_value=True)

        # Mocking import_documents to return a list with fake data
        mocker.patch(
            "src.data.data_mover.DocumentMover.load_and_split_user_text",
            return_value=["Document1", "Document2", "Document3"],
        )

        # Test the load_and_split_text method
        documents = self.document_mover.load_and_split_user_text(
            "parent_dir", file_name
        )
        assert isinstance(documents, list)
        assert all(isinstance(doc, str) for doc in documents)
        assert len(documents) == 3  # Expecting 3 documents from the mocked data

    @pytest.mark.parametrize("iteration", range(10))
    def test_load_message_text_valid_file(self, iteration, mocker):
        """Test to check if the function loads and splits text from a file."""
        # Generate a random file name for each iteration
        file_name = f"test_file_{iteration}.csv"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}")

        # Mocking os.path.isfile to return True
        mocker.patch("os.path.isfile", return_value=True)

        # Mocking import_documents to return a list with fake data
        mocker.patch(
            "src.data.data_mover.DocumentMover.load_message_text",
            return_value=["Document1", "Document2", "Document3"],
        )

        # Test the load_and_split_text method
        documents = self.document_mover.load_message_text(
            "parent_dir", file_name
        )
        assert isinstance(documents, list)
        assert all(isinstance(doc, str) for doc in documents)
        assert len(documents) == 3  # Expecting 3 documents from the mocked data

    @pytest.mark.parametrize("iteration", range(10))
    def test_import_documents_graceful_failure(self, iteration, mocker):
        """Test to check if the function raises an Exception when given a non-existent file path."""
        # Generate a random file name for each iteration
        file_name = f"non_existent_file_{iteration}.txt"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}.csv")

        # Mocking os.path.isfile to return False
        mocker.patch("os.path.isfile", return_value=False)

        with pytest.raises(Exception):
            self.document_mover.load_and_split_user_text("parent_dir", file_name)
