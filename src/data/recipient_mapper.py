import pandas as pd
from src.data.data_validation import validate_columns_in_dataframe, validate_dataframe


class RecipientMapper:
    @staticmethod
    def create_recipient_id_to_name_mapping(df_recipient: pd.DataFrame) -> dict:
        """
        Creates a dictionary to map recipient_id to system_given_name.

        Args:
            df_recipient (pd.DataFrame): Pandas DataFrame containing recipient information.
                It should have columns '_id' and 'profile_joined_name'.

        Returns:
            dict: A dictionary mapping recipient_id to system_given_name.

        Raises:
            TypeError: If df_recipient is not a pandas DataFrame.
            KeyError: If df_recipient is missing required columns '_id' and/or 'profile_joined_name'.
            Exception: If there's an error during dictionary creation.
        """
        # Validate input data
        validate_dataframe(df_recipient)
        validate_columns_in_dataframe(df_recipient, ["_id", "profile_joined_name"])

        try:
            recipient_id_to_name = df_recipient.set_index("_id")[
                "profile_joined_name"
            ].to_dict()
            return recipient_id_to_name

        except Exception as e:
            raise Exception(f"Failed to create recipient ID to name mapping: {str(e)}")
