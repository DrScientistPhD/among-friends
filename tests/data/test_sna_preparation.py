import pandas as pd
import pytest
from faker import Faker

from src.data.sna_preparation import SnaDataWrangler


class TestSnaDataWrangler:
    """Test class for the SnaDataWrangler class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.data_wrangler = SnaDataWrangler()

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_response_react_dataframe_valid(self, iteration, sample_response_react_dataframe):
        """
        Test the standardize_response_react_dataframe function with valid input.
        """
        df = sample_response_react_dataframe
        standardized_df = self.data_wrangler.standardize_response_react_dataframe(df)
        expected_columns = [
            "target_participant_id",
            "target_datetime",
            "source_participant_id",
            "source_datetime",
            "weight",
            "interaction_category"
        ]
        assert set(standardized_df.columns) == set(expected_columns)
        assert (standardized_df["interaction_category"] == "response").all()

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_response_react_dataframe_invalid(self, iteration, sample_response_react_dataframe):
        """
        Test the standardize_response_react_dataframe function with columns missing.
        """
        # List of critical columns
        critical_columns = [
            "comment_from_recipient_id",
            "comment_date_sent_datetime",
            "response_from_recipient_id",
            "response_date_sent_datetime",
            "weight"
        ]

        # Randomly choose a column to remove
        column_to_remove = self.fake.random_element(elements=critical_columns)
        df_invalid = sample_response_react_dataframe.drop(columns=column_to_remove)

        with pytest.raises(Exception):
            self.data_wrangler.standardize_response_react_dataframe(df_invalid)

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_emoji_react_dataframe_valid(self, iteration, sample_emoji_react_dataframe):
        """
        Test the standardize_emoji_react_dataframe function with valid input.
        """
        df = sample_emoji_react_dataframe
        standardized_df = self.data_wrangler.standardize_emoji_react_dataframe(df)
        expected_columns = [
            "target_participant_id",
            "target_datetime",
            "source_participant_id",
            "source_datetime",
            "weight",
            "interaction_category"
        ]
        assert set(standardized_df.columns) == set(expected_columns)
        assert (standardized_df["interaction_category"] == "emoji").all()

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_emoji_react_dataframe_invalid(self, iteration, sample_emoji_react_dataframe):
        """
        Test the standardize_emoji_react_dataframe function with columns missing.
        """
        # List of critical columns
        critical_columns = [
            "comment_from_recipient_id",
            "comment_date_sent_datetime",
            "emoji_author_id",
            "emoji_date_sent_datetime",
            "weight"
        ]

        # Randomly choose a column to remove
        column_to_remove = self.fake.random_element(elements=critical_columns)
        df_invalid = sample_emoji_react_dataframe.drop(columns=column_to_remove)

        with pytest.raises(Exception):
            self.data_wrangler.standardize_emoji_react_dataframe(df_invalid)

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_quotation_react_dataframe_valid(self, iteration, sample_quotation_react_dataframe):
        """
        Test the standardize_quotation_react_dataframe function with valid input.
        """
        df = sample_quotation_react_dataframe
        standardized_df = self.data_wrangler.standardize_quotation_react_dataframe(df)
        expected_columns = [
            "target_participant_id",
            "target_datetime",
            "source_participant_id",
            "source_datetime",
            "weight",
            "interaction_category"
        ]
        assert set(standardized_df.columns) == set(expected_columns)
        assert (standardized_df["interaction_category"] == "quotation").all()

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_quotation_react_dataframe_invalid(self, iteration, sample_quotation_react_dataframe):
        """
        Test the standardize_emoji_react_dataframe function with columns missing.
        """
        # List of critical columns
        critical_columns = [
            "quotation_from_recipient_id",
            "quotation_date_sent_datetime",
            "response_from_recipient_id",
            "response_date_sent_datetime",
            "weight"
        ]

        # Randomly choose a column to remove
        column_to_remove = self.fake.random_element(elements=critical_columns)
        df_invalid = sample_quotation_react_dataframe.drop(columns=column_to_remove)

        with pytest.raises(Exception):
            self.data_wrangler.standardize_quotation_react_dataframe(df_invalid)