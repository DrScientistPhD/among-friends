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
            ].dropna().to_dict()
            return recipient_id_to_name

        except Exception as e:
            raise Exception(f"Failed to create recipient ID to name mapping: {str(e)}")

    @staticmethod
    def update_node_participant_names(
        nodes_edges_df: pd.DataFrame, recipient_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Updates target_participant_id and source_participant_id in nodes_edges_df
        with profile_joined_name from recipient_df based on recipient_id.

        Args:
            nodes_edges_df (pd.DataFrame): DataFrame with columns 'target_participant_id', 'source_participant_id',
            and other relevant columns.
            recipient_df (pd.DataFrame): DataFrame with columns '_id', 'profile_joined_name', and other relevant
            columns.

        Returns:
            pd.DataFrame: Updated nodes_edges_df with profile_joined_name.

        Raises:
            TypeError: If input data is not of expected type.
            KeyError: If nodes_edges_df or recipient_df is missing required columns.
            Exception: If there's an error during ID mapping.
        """
        # Validate input data
        validate_dataframe(nodes_edges_df)
        validate_dataframe(recipient_df)
        validate_columns_in_dataframe(
            nodes_edges_df, ["target_participant_id", "source_participant_id"]
        )
        validate_columns_in_dataframe(recipient_df, ["_id", "profile_joined_name"])

        try:
            # Create the recipient_id to name mapping
            recipient_id_to_name = RecipientMapper.create_recipient_id_to_name_mapping(
                recipient_df
            )

            # Update target_participant_id and source_participant_id using the mapping
            nodes_edges_df["target_participant_id"] = nodes_edges_df[
                "target_participant_id"
            ].map(lambda x: recipient_id_to_name.get(x, x))
            nodes_edges_df["source_participant_id"] = nodes_edges_df[
                "source_participant_id"
            ].map(lambda x: recipient_id_to_name.get(x, x))

            return nodes_edges_df

        except Exception as e:
            raise Exception(
                f"Failed to map participant IDs in nodes_edges_df: {str(e)}"
            )
