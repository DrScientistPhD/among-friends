import pytest
from faker import Faker

from src.data.sna_preparation import SnaDataWrangler


class TestSnaDataWrangler:
    """Test class for the SnaDataWrangler class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.sna_data_wrangler = SnaDataWrangler()

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_response_react_dataframe_valid(self, iteration, fake_response_react_dataframe):
        """
        Test the standardize_response_react_dataframe function with valid input.
        """
        df = fake_response_react_dataframe
        standardized_df = self.sna_data_wrangler.standardize_response_react_dataframe(df)
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
    def test_standardize_response_react_dataframe_invalid(self, iteration, fake_response_react_dataframe):
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
        df_invalid = fake_response_react_dataframe.drop(columns=column_to_remove)

        with pytest.raises(Exception):
            self.sna_data_wrangler.standardize_response_react_dataframe(df_invalid)

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_emoji_react_dataframe_valid(self, iteration, sample_emoji_react_dataframe):
        """
        Test the standardize_emoji_react_dataframe function with valid input.
        """
        df = sample_emoji_react_dataframe
        standardized_df = self.sna_data_wrangler.standardize_emoji_react_dataframe(df)
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
            self.sna_data_wrangler.standardize_emoji_react_dataframe(df_invalid)

    @pytest.mark.parametrize("iteration", range(10))
    def test_standardize_quotation_react_dataframe_valid(self, iteration, fake_quotation_react_dataframe):
        """
        Test the standardize_quotation_react_dataframe function with valid input.
        """
        df = fake_quotation_react_dataframe
        standardized_df = self.sna_data_wrangler.standardize_quotation_react_dataframe(df)
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
    def test_standardize_quotation_react_dataframe_invalid(self, iteration, fake_quotation_react_dataframe):
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
        df_invalid = fake_quotation_react_dataframe.drop(columns=column_to_remove)

        with pytest.raises(Exception):
            self.sna_data_wrangler.standardize_quotation_react_dataframe(df_invalid)

    @pytest.mark.parametrize("iteration", range(10))
    def test_process_data_for_sna_valid(self, iteration, fake_message_slim_df):
        """
        Test the process_data_for_sna function with valid "response" input.
        """
        interaction_type = "response"
        base_value = self.fake.random.uniform(0.1, 10.0)  # Generating a random float between 0.1 and 10.0
        message_slim_df = fake_message_slim_df  # Using the fake_message_slim_df fixture for this example

        # Calling the process_data_for_sna method with the required arguments
        processed_df = self.sna_data_wrangler.process_data_for_sna(interaction_type, base_value, message_slim_df)

        expected_columns = [
            "target_participant_id",
            "target_datetime",
            "source_participant_id",
            "source_datetime",
            "weight",
            "interaction_category"
        ]

        assert set(processed_df.columns) == set(expected_columns)
        assert (processed_df["interaction_category"] == interaction_type).all()

    @pytest.mark.parametrize("iteration", range(10))
    def test_process_data_for_sna_invalid(self, iteration, fake_message_slim_df):
        """
        Test the process_data_for_sna function with invalid input.
        """
        # Invalid interaction type
        with pytest.raises(Exception):
            self.sna_data_wrangler.process_data_for_sna("invalid_type", 1.0, fake_message_slim_df)

        # Invalid data type for interaction_type
        with pytest.raises(TypeError):
            self.sna_data_wrangler.process_data_for_sna(1234, 1.0, fake_message_slim_df)

        # Invalid data type for base_value
        with pytest.raises(TypeError):
            self.sna_data_wrangler.process_data_for_sna("response", "invalid_base_value", fake_message_slim_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_concatenate_dataframes_vertically_valid(self, iteration, fake_message_slim_df):
        """
        Test the concatenate_dataframes_vertically function with valid input.
        """
        n = self.fake.random_int(min=1, max=3)
        dfs = [fake_message_slim_df for _ in range(n)]
        concatenated_df = self.sna_data_wrangler.concatenate_dataframes_vertically(dfs)

        assert len(concatenated_df) == len(fake_message_slim_df) * n
        assert list(concatenated_df.columns) == list(fake_message_slim_df.columns)

    @pytest.mark.parametrize("iteration", range(10))
    def test_concatenate_dataframes_vertically_invalid(self, iteration, fake_message_slim_df):
        """
        Test the concatenate_dataframes_vertically function with invalid input (dataframes with different columns).
        """
        df_with_extra_column = fake_message_slim_df.copy()
        df_with_extra_column["extra_column"] = range(len(df_with_extra_column))
        dfs = [fake_message_slim_df, df_with_extra_column]

        with pytest.raises(Exception):
            self.sna_data_wrangler.concatenate_dataframes_vertically(dfs)
