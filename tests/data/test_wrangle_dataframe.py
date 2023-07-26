import pandas as pd
import pytest
from faker import Faker

from src.data.wrangle_dataframe import DataFrameWrangler, RecipientMapper


class TestRecipientMapper:
    """
    Test class for the RecipientMapper class.
    """

    @pytest.fixture
    def fake_recipient_data(self):
        """
        Fixture to generate fake recipient data as a pandas DataFrame for testing.
        Returns:
            pd.DataFrame: Fake recipient data.
        """
        fake = Faker()
        data = {
            "_id": [fake.random_int(1, 100) for _ in range(10)],
            "profile_joined_name": [fake.name() for _ in range(10)],
        }
        return pd.DataFrame(data)

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.recipient_mapper = RecipientMapper()

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
        Test to check if the function raises a ValueError when an empty DataFrame is provided.
        """
        empty_df = pd.DataFrame()  # Create an empty DataFrame
        with pytest.raises(
            ValueError,
            match="df_recipient is missing required columns '_id' and/or 'profile_joined_name'",
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(empty_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_invalid_dataframe(self, iteration):
        """
        Test to check if the function raises a ValueError when an invalid DataFrame is provided.
        """
        # Create an invalid DataFrame missing the '_id' column
        invalid_df = pd.DataFrame({"profile_joined_name": ["Alice", "Bob", "Charlie"]})
        with pytest.raises(
            ValueError,
            match="df_recipient is missing required columns '_id' and/or 'profile_joined_name'",
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(invalid_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError or TypeError when invalid input is provided.
        """
        # Passing None as input
        with pytest.raises(TypeError, match="df_recipient must be a pandas DataFrame"):
            self.recipient_mapper.create_recipient_id_to_name_mapping(None)

        # Invalid column names in the DataFrame for this test case
        with pytest.raises(
            ValueError,
            match="df_recipient is missing required columns '_id' and/or 'profile_joined_name'",
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(
                pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
            )


class TestDataFrameWrangler:
    """
    Test class for the DataFrameWrangler class.
    """

    @pytest.fixture
    def fake_dataframe_messages(self):
        """
        Fixture to generate fake DataFrame data for testing messages DataFrame.
        Returns:
            pd.DataFrame: Fake DataFrame with random data.
        """
        fake = Faker()
        return pd.DataFrame(
            {
                "_id": [fake.random_int() for _ in range(10)],
                "thread_id": [fake.word() for _ in range(10)],
                "from_recipient_id": [fake.random_int() for _ in range(10)],
                "quote_id": [fake.random_int() for _ in range(10)],
                "date_sent": [fake.date_time() for _ in range(10)],
                "body": [fake.text() for _ in range(10)],
            }
        )

    @pytest.fixture
    def fake_dataframe_reactions(self):
        """
        Fixture to generate fake DataFrame data for testing reactions DataFrame.
        Returns:
            pd.DataFrame: Fake DataFrame with random data.
        """
        fake = Faker()
        return pd.DataFrame(
            {
                "_id": [fake.random_int() for _ in range(10)],
                "message_id": [fake.random_int() for _ in range(10)],
                "author_id": [fake.random_int() for _ in range(10)],
                "emoji": [
                    fake.random_element(elements=("😍", "🐍", "❤️", "👍", "🙌"))
                    for _ in range(10)
                ],
                "date_sent": [fake.date_time() for _ in range(10)],
            }
        )

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.dataframe_wrangler = DataFrameWrangler()

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_messages_df(self, iteration, fake_dataframe_messages):
        """
        Test to check if filter_and_rename_messages_df() filters and renames columns correctly.
        """
        # Rename columns in the fake DataFrame
        fake_dataframe_messages.rename(
            columns={
                "_id": "comment_id",
                "thread_id": "comment_thread_id",
                "from_recipient_id": "comment_from_recipient_id",
                "date_sent": "comment_date_sent",
                "body": "comment_body",
            },
            inplace=True,
        )

        # Set the expected column names after renaming
        expected_columns = [
            "comment_id",
            "comment_thread_id",
            "comment_from_recipient_id",
            "quote_id",
            "comment_date_sent",
            "comment_body",
        ]

        filtered_and_renamed_df = self.dataframe_wrangler.filter_and_rename_messages_df(
            fake_dataframe_messages
        )

        assert isinstance(filtered_and_renamed_df, pd.DataFrame)
        assert list(filtered_and_renamed_df.columns) == expected_columns

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_messages_df_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError when invalid input is provided.
        """
        # Passing a non-DataFrame input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_messages_df("invalid_data")

        # Passing None as input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_messages_df(None)

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_reactions_df(self, iteration, fake_dataframe_reactions):
        """
        Test to check if filter_and_rename_reactions_df() filters and renames columns correctly.
        """
        # Rename columns in the fake DataFrame
        fake_dataframe_reactions.rename(
            columns={
                "_id": "reaction_id",
                "author_id": "reaction_author_id",
                "date_sent": "reaction_date_sent",
            },
            inplace=True,
        )

        # Set the expected column names after renaming
        expected_columns = [
            "reaction_id",
            "message_id",
            "reaction_author_id",
            "emoji",
            "reaction_date_sent",
            "reaction_translation",
        ]

        filtered_and_renamed_df = (
            self.dataframe_wrangler.filter_and_rename_reactions_df(
                fake_dataframe_reactions
            )
        )

        assert isinstance(filtered_and_renamed_df, pd.DataFrame)
        assert list(filtered_and_renamed_df.columns) == expected_columns

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_reactions_df_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError when invalid input is provided.
        """
        # Passing a non-DataFrame input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_reactions_df("invalid_data")

        # Passing None as input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_reactions_df(None)
