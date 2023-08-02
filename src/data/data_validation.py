import pandas as pd


def validate_dataframe(df):
    """
    Validates that the input is a pandas DataFrame.

    Args:
        df: The object to validate.

    Raises:
        TypeError: If df is not a pandas DataFrame.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected 'df' to be a pandas DataFrame, but got {type(df)}")


def validate_columns_in_dataframe(df, columns):
    """
    Validates that the specified columns exist in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        columns (list or set): The names of the columns to check.

    Raises:
        ValueError: If any of the columns are not found in the DataFrame.
    """
    missing_columns = set(columns) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"DataFrame is missing the following required columns: {missing_columns}"
        )


def validate_data_types(data, expected_type, argument_label):
    """
    Validates that the data is of the expected type.

    Args:
        data: The data to validate.
        expected_type (type): The expected type of the data.
        argument_label (str): Label for the data, used in error messages.

    Raises:
        TypeError: If data is not of the expected type.
    """
    if not isinstance(data, expected_type):
        raise TypeError(
            f"Expected '{argument_label}' to be a {expected_type}, but got {type(data)}"
        )
