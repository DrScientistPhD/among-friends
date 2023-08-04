import pandas as pd
import pytest
from faker import Faker

from src.data.data_wrangling import (
    MessageDataWrangler,
    QuotationResponseDataWrangler,
    ReactionDataWrangler,
)


class TestMessageDataWrangler:
    """
    Test class for the DataFrameWrangler class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.message_data_wrangler = MessageDataWrangler()

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_messages_df(self, iteration, fake_dataframe_messages):
        """
        Test to check if filter_and_rename_messages_df() filters and renames columns correctly.
        """
        # Make a copy of the DataFrame before renaming columns
        df_copy = fake_dataframe_messages.copy()

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

        filtered_and_renamed_df = (
            self.message_data_wrangler.filter_and_rename_messages_df(df_copy)
        )

        assert isinstance(filtered_and_renamed_df, pd.DataFrame)
        assert list(filtered_and_renamed_df.columns) == expected_columns

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_messages_df_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError when invalid input is provided.
        """
        # Passing a non-DataFrame input
        with pytest.raises(TypeError):
            self.message_data_wrangler.filter_and_rename_messages_df("invalid_data")

        # Passing None as input
        with pytest.raises(TypeError):
            self.message_data_wrangler.filter_and_rename_messages_df(None)


class TestReactionDataWrangler:
    """
    Test class for the DataFrameWrangler class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.reaction_data_wrangler = ReactionDataWrangler()

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_reactions_df(self, iteration, fake_dataframe_reactions):
        """
        Test to check if filter_and_rename_reactions_df() filters and renames columns correctly.
        """
        # Make a copy of the DataFrame before renaming columns
        df_copy = fake_dataframe_reactions.copy()

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

        # Pass the copy of the DataFrame to the function
        filtered_and_renamed_df = (
            self.reaction_data_wrangler.filter_and_rename_reactions_df(df_copy)
        )

        assert isinstance(filtered_and_renamed_df, pd.DataFrame)
        assert list(filtered_and_renamed_df.columns) == expected_columns

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_reactions_df_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError when invalid input is provided.
        """
        # Passing a non-DataFrame input
        with pytest.raises(TypeError):
            self.reaction_data_wrangler.filter_and_rename_reactions_df("invalid_data")

        # Passing None as input
        with pytest.raises(TypeError):
            self.reaction_data_wrangler.filter_and_rename_reactions_df(None)


class TestQuotationResponseDataWrangler:
    """
    Test class for the DataFrameWrangler class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.quotation_response_data_wrangler = QuotationResponseDataWrangler()

    @pytest.fixture
    def fake_dataframe_messages(self):
        """
        Fixture to generate fake DataFrame data for testing messages DataFrame.
        Returns:
            pd.DataFrame: Fake DataFrame with random data.
        """
        ids = [self.fake.random_int(min=1, max=1000) for _ in range(100)]
        return pd.DataFrame(
            {
                "_id": ids,
                "date_sent": pd.date_range(start="1/1/2018", periods=100),
                "thread_id": [
                    self.fake.random_int(min=1, max=1000) for _ in range(100)
                ],
                "from_recipient_id": [
                    self.fake.random_int(min=1, max=1000) for _ in range(100)
                ],
                "body": [self.fake.sentence(nb_words=6) for _ in range(100)],
                "quote_id": ids,  # Using ids as quote_id to mimic reference to another message
            }
        )

    @pytest.fixture
    def fake_quotation_response_df(self):
        """
        Fixture to generate fake DataFrame data for testing quotation-response DataFrame.
        Returns:
            pd.DataFrame: Fake DataFrame with random data.
        """
        n = 100

        # Generate 'quotation_date_sent' and 'response_date_sent' first
        quotation_date_sent = [
            self.fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
        ]
        response_date_sent = [
            self.fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
        ]

        # Calculate the time difference in seconds and store in 'time_diff'
        time_diff = [
            (start - end) / 1000
            for start, end in zip(quotation_date_sent, response_date_sent)
        ]

        data = {
            "quotation_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=True
            ),
            "quotation_date_sent": quotation_date_sent,
            "quotation_thread_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "quotation_from_recipient_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "quotation_body": self.fake.sentences(nb=n, ext_word_list=None),
            "response_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=True
            ),
            "response_date_sent": response_date_sent,
            "response_thread_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "response_from_recipient_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "response_body": self.fake.sentences(nb=n, ext_word_list=None),
            "quote_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "time_diff": time_diff,
        }

        return pd.DataFrame(data)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_quotation_response_df_no_quotation(
        self, iteration, fake_dataframe_messages
    ):
        """
        Test to check if create_quotation_response_df() correctly handles the case where a message has no quotation.
        """
        # Modify the fake DataFrame so that the first entry has no quotation
        fake_dataframe_messages.loc[0, "quote_id"] = None

        quotation_response_df = (
            self.quotation_response_data_wrangler.create_quotation_response_df(
                fake_dataframe_messages
            )
        )

        # If there are no quotations, the DataFrame should be empty
        assert quotation_response_df.empty

    @pytest.mark.parametrize("iteration", range(1))
    def test_create_quotation_response_df_invalid_input(self, iteration):
        """
        Test to check if create_quotation_response_df() raises an error when given invalid input.
        """
        # Try calling create_quotation_response_df() with non-DataFrame inputs
        with pytest.raises(TypeError):
            self.quotation_response_data_wrangler.create_quotation_response_df(
                "invalid_input"
            )
