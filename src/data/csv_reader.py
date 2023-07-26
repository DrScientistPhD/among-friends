import pandas as pd


class CSVReader:
    @staticmethod
    def read_csv_as_dataframe(file_name: str) -> pd.DataFrame:
        """
        Reads a CSV file as a pandas DataFrame.

        Args:
            file_name (str): The name of the CSV file.

        Returns:
            pd.DataFrame: The pandas DataFrame containing the data from the CSV file.

        Raises:
            ValueError: If the file_path is not a string.
            pd.errors.EmptyDataError: If the CSV file is empty or cannot be read.
        """
        # Using isinstance to validate the input argument.
        if not isinstance(file_name, str):
            raise TypeError("file_path must be a string")

        try:
            # Reading the CSV file into a DataFrame.
            dataframe = pd.read_csv(
                "/Users/raymondpasek/Repos/among-friends/data/raw/message.csv"
                + file_name
            )

            return dataframe
        except pd.errors.EmptyDataError as e:
            raise Exception(f"Error while reading the CSV file: {e}")
