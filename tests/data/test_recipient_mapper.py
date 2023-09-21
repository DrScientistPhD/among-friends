import random

import pandas as pd
import pytest
from faker import Faker

from src.data.recipient_mapper import RecipientMapper


class TestRecipientMapper:
    """
    Test class for the EmojiTranslator class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.recipient_mapper = RecipientMapper()

    @pytest.fixture
    def fake_recipient_data(self):
        """
        Fixture to generate fake recipient data as a pandas DataFrame for testing.
        Returns:
            pd.DataFrame: Fake recipient data.
        """
        data = {
            "_id": [self.fake.random_int(1, 100) for _ in range(10)],
            "profile_joined_name": [self.fake.name() for _ in range(10)],
        }
        return pd.DataFrame(data)

    @pytest.fixture
    def fake_nodes_edges_data(self, fake_recipient_data):
        """
        Fixture to generate fake nodes_edges data as a pandas DataFrame for testing.
        Returns:
            pd.DataFrame: Fake nodes_edges data.
        """
        # Ensure that IDs used in nodes_edges_data are from the recipient_data
        recipient_ids = fake_recipient_data["_id"].tolist()
        data = {
            "target_participant_id": [random.choice(recipient_ids) for _ in range(10)],
            "source_participant_id": [random.choice(recipient_ids) for _ in range(10)],
        }
        return pd.DataFrame(data)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping(self, iteration, fake_recipient_data):
        """
        Test to check if the function returns the correct recipient_id to system_given_name dictionary.
        """
        # Calling the function with the fake recipient data.
        recipient_id_to_name = (
            self.recipient_mapper.create_recipient_id_to_name_mapping(
                fake_recipient_data
            )
        )

        # Ensure the recipient_id_to_name dictionary is correct.
        expected_dict = {
            row["_id"]: row["profile_joined_name"]
            for _, row in fake_recipient_data.iterrows()
        }
        assert recipient_id_to_name == expected_dict

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_empty_dataframe(self, iteration):
        """
        Test to check if the function raises an Exception when an empty DataFrame is provided.
        """
        empty_df = pd.DataFrame()  # Create an empty DataFrame
        with pytest.raises(Exception):
            self.recipient_mapper.create_recipient_id_to_name_mapping(empty_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_invalid_dataframe(self, iteration):
        """
        Test to check if the function raises a Exception when an invalid DataFrame is provided.
        """
        # Create an invalid DataFrame missing the '_id' column
        invalid_df = pd.DataFrame({"profile_joined_name": ["Alice", "Bob", "Charlie"]})
        with pytest.raises(Exception):
            self.recipient_mapper.create_recipient_id_to_name_mapping(invalid_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError or TypeError when invalid input is provided.
        """
        # Passing None as input
        with pytest.raises(TypeError):
            self.recipient_mapper.create_recipient_id_to_name_mapping(None)

        # Invalid column names in the DataFrame for this test case
        with pytest.raises(KeyError):
            self.recipient_mapper.create_recipient_id_to_name_mapping(
                pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
            )

    @pytest.mark.parametrize("iteration", range(10))
    def test_update_node_participant_names(
        self, iteration, fake_nodes_edges_data, fake_recipient_data
    ):
        """
        Test to check if the function correctly updates the participant names.
        """
        # Calling the function with the fake data
        updated_df = self.recipient_mapper.update_node_participant_names(
            fake_nodes_edges_data, fake_recipient_data
        )

        # Create the expected updated DataFrame
        recipient_id_to_name = fake_recipient_data.set_index("_id")[
            "profile_joined_name"
        ].to_dict()
        expected_df = fake_nodes_edges_data.copy()
        expected_df["target_participant_id"] = expected_df["target_participant_id"].map(
            lambda x: recipient_id_to_name.get(x, x)
        )
        expected_df["source_participant_id"] = expected_df["source_participant_id"].map(
            lambda x: recipient_id_to_name.get(x, x)
        )

        assert updated_df.equals(expected_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_update_node_participant_names_general_exception(
        self, iteration, mocker, fake_nodes_edges_data, fake_recipient_data
    ):
        """
        Test to check if the function raises the general exception when something unexpected goes wrong.
        """
        # Mock the map method of pandas Series to raise an exception
        mocker.patch.object(
            pd.Series, "map", side_effect=Exception("Unexpected exception!")
        )

        with pytest.raises(
            Exception,
            match="Failed to map participant IDs in nodes_edges_df: Unexpected exception!",
        ):
            self.recipient_mapper.update_node_participant_names(
                fake_nodes_edges_data, fake_recipient_data
            )
