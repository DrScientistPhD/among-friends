from src.data.data_validation import validate_data_types
import os
import pandas as pd

class CSVImporter:
    @staticmethod
    def import_csv(file_name: str) -> pd.DataFrame:
        """
        Imports a CSV file from the data/raw directory of the repository as a dataframe.

        Args:
            file_name (str): The name of the CSV file to be imported.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the CSV file.

        Raises:
            TypeError: If file_name is not of the expected type (str).
            FileNotFoundError: If the file does not exist in the data/raw directory.
            Exception: If there's an error during the import process.
        """
        # Validate input data
        validate_data_types(file_name, str, "file_name")

        path_to_file = f"data/raw/{file_name}.csv"
        try:
            # Check if the file exists
            if not os.path.isfile(path_to_file):
                raise FileNotFoundError(f"The file {file_name}.csv does not exist in the data/raw directory.")

            # Read the CSV file into a DataFrame
            df = pd.read_csv(path_to_file)
            return df

        except FileNotFoundError as fnfe:
            raise fnfe

        except Exception as e:
            raise Exception(f"Failed to import CSV file: {str(e)}")
