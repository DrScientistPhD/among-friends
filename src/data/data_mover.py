import os
from typing import List

import pandas as pd
from langchain.docstore.document import Document
from langchain.document_loaders import TextLoader
from langchain.text_splitter import CharacterTextSplitter

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


class TextMover:
    @staticmethod
    def import_text_file(parent_directory: str, file_name: str) -> List[str]:
        """
        Imports text data from a file.

        Args:
            parent_directory (str): The parent directory where the text file is located.
            file_name (str): The name of the text file to be imported.

        Returns:
            List[str]: A list of strings read from the file, where each string represents a line.

        Raises:
            TypeError: If either parent_directory or file_name is not of the expected type (str).
            FileNotFoundError: If the file does not exist.
            Exception: If there's an error during the import process.
        """
        # Validate input data
        if not isinstance(parent_directory, str) or not isinstance(file_name, str):
            raise TypeError("Parent directory and file name should be strings.")

        full_file_path = os.path.join(parent_directory, f"{file_name}.txt")
        try:
            # Check if the file exists
            if not os.path.isfile(full_file_path):
                raise FileNotFoundError(f"The file {file_name} does not exist.")

            # Read the text file
            with open(full_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                # Strip newline characters and create a list of strings
                text_data = [line.strip() for line in lines]

            return text_data

        except Exception as e:
            raise Exception(f"Failed to import text file: {str(e)}")

    @staticmethod
    def export_sentences_to_file(
        sentences: List[str], parent_directory: str, file_name: str
    ) -> None:
        """
        Exports a list of sentences to a text file in the specified directory.

        Args:
            sentences (List[str]): The list of sentences to be exported.
            parent_directory (str): The parent directory where the text file should be saved.
            file_name: The name of the text file to be created.

        Raises:
            TypeError: If sentences, parent_directory, or file_name is not of the expected type.
            Exception: If there's an error during the export process.
        """
        # Validate input data types
        if not isinstance(sentences, list) or not all(
            isinstance(sentence, str) for sentence in sentences
        ):
            raise TypeError("Sentences should be a list of strings.")

        if not isinstance(parent_directory, str) or not isinstance(file_name, str):
            raise TypeError("Parent directory and file name should be strings.")

        path_to_file = os.path.join("data", parent_directory, f"{file_name}.txt")

        try:
            with open(path_to_file, "w", encoding="utf-8") as f:
                for sentence in sentences:
                    f.write(sentence + "\n")
        except Exception as e:
            raise Exception(f"Failed to export sentences to text file: {str(e)}")


class DocumentMover:
    @staticmethod
    def load_and_split_text(parent_directory: str, file_name: str) -> List[Document]:
        """
        Loads text data from a file and splits it into a list of individual documents.

        Args:
            parent_directory (str): The parent directory where the text file is located.
            file_name (str): The name of the text file to be imported.

        Returns:
            List[Document]: A document containing a list of individual documents.

        Raises:
            TypeError: If either parent_directory or file_name is not of the expected type (str).
            FileNotFoundError: If the file does not exist.
            Exception: If there's an error during the import process.
        """
        # Validate input data
        if not isinstance(parent_directory, str) or not isinstance(file_name, str):
            raise TypeError("Parent directory and file name should be strings.")

        full_file_path = os.path.join(parent_directory, f"{file_name}.txt")
        try:
            # Check if the file exists
            if not os.path.isfile(full_file_path):
                raise FileNotFoundError(f"The file {file_name} does not exist.")

            # Load the text from the file
            loader = TextLoader(
                os.path.join(parent_directory, f"{file_name}.txt")
            )
            documents = loader.load()

            # Add the source file name to the metadata
            [doc.metadata.update({'source': file_name}) for doc in documents]

            # Split the text by new line
            text_splitter = CharacterTextSplitter(
                separator="\n",
                # We are only interested splitting by new line. We don't care about the chunk size so long
                # as the chunk contains the entire new line. Ignore the warning this results in.
                chunk_size=1,
                chunk_overlap=0,
            )

            docs = text_splitter.split_documents(documents)

            return docs

        except Exception as e:
            raise Exception(f"Failed to import text file: {str(e)}")
