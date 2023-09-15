import pandas as pd
import pytest
from faker import Faker
from src.data.csv_mover import CSVMover


class TestCSVImporter:
    """Test class for the CSVImporter class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.csv_importer = CSVMover()

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
        df = self.csv_importer.import_csv("parent_dir", file_name)
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
            self.csv_importer.import_csv("parent_dir", file_name)