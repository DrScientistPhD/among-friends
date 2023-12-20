import json
import os

import pandas as pd

from src.data.data_validation import validate_data_types


class CSVMover:
    @staticmethod
    def import_csv(parent_directory: str, file_name: str) -> pd.DataFrame:
        """
        Imports a CSV file as a dataframe.

        Args:
            parent_directory (str): The parent directory where the CSV file is located.
            file_name (str): The name of the CSV file to be imported.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the CSV file.

        Raises:
            TypeError: If either parent_directory or file_name is not of the expected type (str).
            FileNotFoundError: If the file does not exist.
            Exception: If there's an error during the import process.
        """
        # Validate input data
        validate_data_types(parent_directory, str, "parent_directory")
        validate_data_types(file_name, str, "file_name")

        full_file_path = os.path.join(parent_directory, f"{file_name}.csv")
        try:
            # Check if the file exists
            if not os.path.isfile(full_file_path):
                raise FileNotFoundError(f"The file {file_name}.csv does not exist.")

            # Read the CSV file into a DataFrame
            df = pd.read_csv(full_file_path)
            return df

        except pd.errors.ParserError as pe:
            raise pe

        except Exception as e:
            raise Exception(f"Failed to import CSV file: {str(e)}")

    @staticmethod
    def export_csv(df: pd.DataFrame, parent_directory: str, file_name: str) -> None:
        """
        Exports a DataFrame to a CSV file in the specified directory of the repository.

        Args:
            df (pd.DataFrame): The DataFrame to be exported.
            parent_directory (str): The parent directory where the CSV file should be saved.
            file_name (str): The name of the CSV file to be created.

        Raises:
            TypeError: If df, parent_directory, or file_name is not of the expected type.
            Exception: If there's an error during the export process.
        """
        # Validate input data
        validate_data_types(df, pd.DataFrame, "df")
        validate_data_types(parent_directory, str, "parent_directory")
        validate_data_types(file_name, str, "file_name")

        path_to_file = os.path.join("data", parent_directory, f"{file_name}.csv")
        try:
            # Write the DataFrame to a CSV file
            df.to_csv(path_to_file, index=False)
        except Exception as e:
            raise Exception(f"Failed to export DataFrame to CSV file: {str(e)}")


class JSONMover:
    @staticmethod
    def import_json(parent_directory: str, file_name: str) -> str:
        """
        Imports JSON data from a file.

        Args:
            parent_directory (str): The parent directory where the JSON file is located.
            file_path (str): The name of the JSON file to be imported.

        Returns:
            str: The JSON data read from the file.

        Raises:
            TypeError: If either parent_directory or file_name is not of the expected type (str).
            FileNotFoundError: If the file does not exist.
            Exception: If there's an error during the import process.
        """
        # Validate input data
        validate_data_types(parent_directory, str, "parent_directory")
        validate_data_types(file_name, str, "file_name")

        full_file_path = os.path.join(parent_directory, f"{file_name}.json")
        try:
            # Check if the file exists
            if not os.path.isfile(full_file_path):
                raise FileNotFoundError(f"The file {file_name}.json does not exist.")

            # Read the JSON file
            with open(full_file_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)
            return json_data

        except json.JSONDecodeError as jde:
            raise jde

        except Exception as e:
            raise Exception(f"Failed to import JSON file: {str(e)}")

    @staticmethod
    def export_json(data_object: str, parent_directory: str, file_name: str) -> None:
        """
        Exports a string to a JSON file in the specified directory of the repository.

        Args:
            data_object (str): The data object to be exported.
            parent_directory (str): The parent directory where the JSON file should be saved.
            file_name: The name of the JSON file to be created.

        Raises:
            TypeError: If data_object, parent_directory, or file_name is not of the expected type.
            Exception: If there's an error during the export process.
        """
        # Validate input data
        validate_data_types(data_object, str, "data_object")
        validate_data_types(parent_directory, str, "parent_directory")
        validate_data_types(file_name, str, "file_name")

        path_to_file = os.path.join("data", parent_directory, f"{file_name}.json")
        try:
            with open(path_to_file, "w", encoding="utf-8") as f:
                json.dump(data_object, f, ensure_ascii=False)
        except Exception as e:
            raise Exception(f"Failed to export data to JSON file: {str(e)}")
