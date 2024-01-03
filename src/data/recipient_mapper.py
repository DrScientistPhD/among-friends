import pandas as pd

from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_dataframe,
    validate_data_types,
)


class RecipientMapper:
    def __init__(self, default_author_name: str):
        self.default_author_name = default_author_name

    def create_recipient_id_to_name_mapping(
        self, df_recipient: pd.DataFrame, name_field: str
    ) -> dict:
        """
        Creates a dictionary to map recipient_id to system_given_name.

        Args:
            df_recipient (pd.DataFrame): Pandas DataFrame containing recipient information.
                It should have columns '_id' and 'profile_joined_name'.
            name_field (str): The name of the column containing the name to be used for mapping.

        Returns:
            dict: A dictionary mapping recipient_id to system_given_name.

        Raises:
            TypeError: If df_recipient is not a pandas DataFrame, or if name_field is not a string.
            KeyError: If df_recipient is missing required columns '_id' and/or 'profile_joined_name'.
            Exception: If there's an error during dictionary creation.
        """
        # Validate input data
        validate_dataframe(df_recipient)
        validate_data_types(name_field, str, "name_field")
        validate_columns_in_dataframe(df_recipient, ["_id", "profile_joined_name"])

        if not any(
            name_field == field
            for field in ["system_display_name", "profile_joined_name"]
        ):
            raise ValueError(
                f"Invalid name_field. Use 'system_display_name' or 'profile_joined_name'."
            )

        if self.default_author_name not in df_recipient["profile_joined_name"].values:
            raise ValueError(
                f"Default author name '{self.default_author_name}' not found."
            )

        try:
            # Set the default_author_name value to the system_display_name where default_author_name is the
            # profile_joined_name
            df_recipient.loc[
                df_recipient["profile_joined_name"] == self.default_author_name,
                "system_display_name",
            ] = self.default_author_name

            recipient_id_to_name = (
                df_recipient.set_index("_id")[name_field].dropna().to_dict()
            )
            return recipient_id_to_name

        except Exception as e:
            raise Exception(f"Failed to create recipient ID to name mapping: {str(e)}")

    def update_node_participant_names(
        self, nodes_edges_df: pd.DataFrame, recipient_df: pd.DataFrame
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
                self, recipient_df, "profile_joined_name"
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
