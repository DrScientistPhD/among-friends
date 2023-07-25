import pandas as pd


class CSVReader:
    @staticmethod
    def read_csv_as_dataframe(file_path: str) -> pd.DataFrame:
        """
        Reads a CSV file as a pandas DataFrame.

        Args:
            file_path (str): The path to the CSV file.

        Returns:
            pd.DataFrame: The pandas DataFrame containing the data from the CSV file.

        Raises:
            ValueError: If the file_path is not a string.
            pd.errors.EmptyDataError: If the CSV file is empty or cannot be read.
        """
        # Using isinstance to validate the input argument.
        if not isinstance(file_path, str):
            raise ValueError("file_path must be a string")

        try:
            # Reading the CSV file into a DataFrame.
            dataframe = pd.read_csv(file_path)

            return dataframe
        except pd.errors.EmptyDataError as e:
            raise ValueError(f"Error while reading the CSV file: {e}")
