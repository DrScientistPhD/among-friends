import pandas as pd
import pytest
from faker import Faker

from src.data.csv_reader import CSVReader


class TestCSVReader:
    """
    Test class for the CSVReader class.
    """

    @pytest.fixture
    def fake_csv_data(self):
        """
        Fixture to generate fake CSV data for testing.
        Returns:
            str: CSV data as a string.
        """
        fake = Faker()
        header = ["Name", "Age", "City"]
        rows = [
            ",".join([fake.name(), str(fake.random_int(18, 65)), fake.city()])
            for _ in range(10)
        ]
        return "\n".join([",".join(header)] + rows)

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.csv_reader = CSVReader()

    @pytest.mark.parametrize("iteration", range(10))
    def test_read_csv_as_dataframe_type(self, iteration, fake_csv_data, mocker):
        """
        Test to check if the output of read_csv_as_dataframe() is a pandas DataFrame.
        """
        # Mocking read_csv to return a DataFrame with fake data.
        mocker.patch("pandas.read_csv", return_value=pd.DataFrame())

        file_path = "dummy_file.csv"
        df = self.csv_reader.read_csv_as_dataframe(file_path)  # Using the class method
        assert isinstance(df, pd.DataFrame)

    @pytest.mark.parametrize("iteration", range(10))
    def test_read_csv_as_dataframe_graceful_failure(self, iteration, mocker):
        """
        Test to check if the function raises a ValueError when given a non-existent file path.
        """
        # Generate a random file name with different extensions for each iteration.
        file_name = f"non_existent_file_{iteration}{self.fake.file_extension()}"

        # Mocking read_csv to raise pd.errors.EmptyDataError.
        mocker.patch("pandas.read_csv", side_effect=pd.errors.EmptyDataError)

        with pytest.raises(ValueError):
            self.csv_reader.read_csv_as_dataframe(file_name)  # Using the class method
