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
        with pytest.raises(
            Exception
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(empty_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_invalid_dataframe(self, iteration):
        """
        Test to check if the function raises a Exception when an invalid DataFrame is provided.
        """
        # Create an invalid DataFrame missing the '_id' column
        invalid_df = pd.DataFrame({"profile_joined_name": ["Alice", "Bob", "Charlie"]})
        with pytest.raises(
            Exception
        ):
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
        with pytest.raises(
            KeyError
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(
                pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
            )