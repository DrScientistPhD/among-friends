import pandas as pd
import pytest
from faker import Faker

from src.data.csv_mover import CSVMover


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
        """Test to check if the function raises a FileNotFoundError when given a non-existent file path."""
        # Generate a random file name for each iteration
        file_name = f"non_existent_file_{iteration}.csv"

        # Mocking os.path.join to construct the full file path
        mocker.patch("os.path.join", return_value=f"parent_dir/{file_name}.csv")

        # Mocking os.path.isfile to return False
        mocker.patch("os.path.isfile", return_value=False)

        with pytest.raises(FileNotFoundError):
            self.csv_mover.import_csv("parent_dir", file_name)

    @pytest.mark.parametrize("iteration", range(10))
    def test_export_csv_success(self, iteration, fake_nodes_edges_dataframe, mocker):
        """Test to check if the function exports a DataFrame to a CSV file successfully."""
        df = fake_nodes_edges_dataframe
        parent_directory = "parent_dir"
        file_name = "test_file"

        # Mocking validate_data_types to bypass actual validation
        mocker.patch("src.data.csv_mover.validate_data_types", return_value=None)

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
