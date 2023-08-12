import pandas as pd
import pytest
from faker import Faker

from src.data.data_wrangling import (DateTimeConverter, MessageDataWrangler,
                                     QuotationResponseDataWrangler,
                                     ReactionDataWrangler)


class TestDateTimeConverter:
    """Test class for the DateTimeConverter class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.date_time_converter = DateTimeConverter()

    @pytest.mark.parametrize("iteration", range(10))
    def test_convert_unix_to_datetime(self, iteration):
        """Test to check if the convert_unix_to_datetime() function properly converts Unix timestamps."""
        # Generate fake Unix timestamps in milliseconds
        unix_timestamps = [self.fake.unix_time() * 1000 for _ in range(10)]

        # Create a DataFrame with the Unix timestamps
        df = pd.DataFrame({"timestamp": unix_timestamps})

        # Use the convert_unix_to_datetime function
        new_df = self.date_time_converter.convert_unix_to_datetime(df, "timestamp")

        # Check if the new column is created
        assert f"timestamp_datetime" in new_df.columns

        # Check if the new column contains valid datetime objects
        assert pd.api.types.is_datetime64_any_dtype(new_df["timestamp_datetime"])

    def test_convert_unix_to_datetime_invalid_column(self):
        """Test to check if the function raises a KeyError when given a non-existent column."""
        df = pd.DataFrame({"other_column": [1234567890]})

        with pytest.raises(KeyError):
            self.date_time_converter.convert_unix_to_datetime(df, "timestamp")

    def test_convert_unix_to_datetime_non_integer_column(self):
        """Test to check if the function raises a TypeError when given a non-integer column."""
        df = pd.DataFrame({"timestamp": ["1234567890"]})

        with pytest.raises(Exception):
            self.date_time_converter.convert_unix_to_datetime(df, "timestamp")


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
    def test_filter_and_rename_messages_df(self, iteration, fake_message_df):
        """
        Test to check if filter_and_rename_messages_df() filters and renames columns correctly.
        """
        # Make a copy of the DataFrame before renaming columns
        df_copy = fake_message_df.copy()

        # Rename columns in the fake DataFrame
        fake_message_df.rename(
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

    @pytest.mark.parametrize("iteration", range(10))
    def test_concatenate_comment_threads_valid(
        self, fake_message_slim_df, iteration
    ):
        """
        Test to check if the function returns the expected DataFrame when valid input is provided.
        """
        result = self.message_data_wrangler.concatenate_comment_threads(
            fake_message_slim_df, group_participants_n=3
        )
        assert isinstance(result, pd.DataFrame)
        assert result.shape[1] == 10
        expected_columns = [
            "comment_id",
            "comment_date_sent",
            "comment_thread_id",
            "comment_from_recipient_id",
            "comment_body",
            "quote_id",
            "response_id",
            "response_date_sent",
            "response_from_recipient_id",
            "response_body",
        ]
        assert list(result.columns) == expected_columns
        assert result["comment_id"].min() >= 1
        assert result["comment_id"].max() <= 1000

    @pytest.mark.parametrize("iteration", range(10))
    def test_concatenate_comment_threads_invalid(self, iteration):
        """
        Test to check if the function raises appropriate exceptions when invalid input is provided.
        """
        group_n = self.fake.random_int(min=1, max=15)

        with pytest.raises(TypeError):
            self.message_data_wrangler.concatenate_comment_threads(
                "invalid_input", group_participants_n=group_n
            )

        invalid_df = pd.DataFrame({"invalid_column": [1, 2, 3]})
        with pytest.raises(ValueError):
            self.message_data_wrangler.concatenate_comment_threads(
                invalid_df, group_participants_n=group_n
            )


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
    def test_filter_and_rename_reactions_df(self, iteration, fake_reaction_df):
        """
        Test to check if filter_and_rename_reactions_df() filters and renames columns correctly.
        """
        # Make a copy of the DataFrame before renaming columns
        df_copy = fake_reaction_df.copy()

        # Rename columns in the fake DataFrame
        fake_reaction_df.rename(
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

    @pytest.mark.parametrize("iteration", range(10))
    def test_merge_message_with_reaction_valid(self, fake_message_slim_df, fake_reaction_slim_df, iteration):
        """
        Test to check if the function correctly merges two dataframes with valid input.
        """
        result = self.reaction_data_wrangler.merge_message_with_reaction(fake_message_slim_df, fake_reaction_slim_df)
        assert isinstance(result, pd.DataFrame)
        assert "comment_id" in result.columns
        assert "message_id" in result.columns
        assert len(result.columns) == len(fake_message_slim_df.columns) + len(fake_reaction_slim_df.columns)

    @pytest.mark.parametrize("iteration", range(10))
    def test_merge_message_with_reaction_invalid(self, iteration):
        """
        Test to check if the function raises appropriate exceptions with invalid input.
        """
        # Passing a non-DataFrame input for message dataframe
        with pytest.raises(TypeError):
            self.reaction_data_wrangler.merge_message_with_reaction("invalid_input", pd.DataFrame())

        # Passing a non-DataFrame input for reaction dataframe
        with pytest.raises(TypeError):
            self.reaction_data_wrangler.merge_message_with_reaction(pd.DataFrame(), "invalid_input")

        # Passing DataFrames without required columns
        invalid_message_df = pd.DataFrame({"invalid_column": [1, 2, 3]})
        invalid_reaction_df = pd.DataFrame({"another_invalid_column": [1, 2, 3]})
        with pytest.raises(KeyError):
            self.reaction_data_wrangler.merge_message_with_reaction(invalid_message_df, pd.DataFrame())
        with pytest.raises(KeyError):
            self.reaction_data_wrangler.merge_message_with_reaction(pd.DataFrame(), invalid_reaction_df)


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

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_quotation_response_df_with_quotation(self, iteration, fake_message_slim_df):
        """
        Test to check if create_quotation_response_df() correctly handles the case where a message has quotations.
        """
        # Assuming that fake_message_slim_df fixture contains both messages with and without quotations

        # Call the create_quotation_response_df method with the fake DataFrame
        quotation_response_df = (
            self.quotation_response_data_wrangler.create_quotation_response_df(
                fake_message_slim_df
            )
        )

        # Verify the structure of the output DataFrame
        expected_columns = [
            "quotation_id", "quotation_date_sent", "quotation_thread_id",
            "quotation_from_recipient_id", "quotation_body", "quote_id",
            "response_id", "response_date_sent", "response_from_recipient_id",
            "comment_from_recipient_id", "response_body"
        ]
        assert list(quotation_response_df.columns) == expected_columns

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_quotation_response_df_no_quotation(
        self, iteration, fake_message_slim_df
    ):
        """
        Test to check if create_quotation_response_df() correctly handles the case where a message has no quotation.
        """
        # Modify the fake DataFrame so that the first entry has no quotation
        fake_message_slim_df.loc[0, "quote_id"] = None

        quotation_response_df = (
            self.quotation_response_data_wrangler.create_quotation_response_df(
                fake_message_slim_df
            )
        )

        # If there are no quotations, the DataFrame should be empty
        assert quotation_response_df.empty

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_quotation_response_df_invalid_input(self, iteration):
        """
        Test to check if create_quotation_response_df() raises an error when given invalid input.
        """
        # Try calling create_quotation_response_df() with non-DataFrame inputs
        with pytest.raises(TypeError):
            self.quotation_response_data_wrangler.create_quotation_response_df(
                "invalid_input"
            )
