import json

import pandas as pd
import pytest
from faker import Faker

from src.data.genai_preparation import GenAiDataWrangler


class TestGenAiDataWrangler:
    """Test class for the GenAiDataWrangler class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.genai_data_wrangler = GenAiDataWrangler()
        self.thread_id_n = self.fake.random_int(min=1, max=3)

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_long_text_long_text(self, iteration):
        """Test that filter_long_text() filters out long text and replaces it with ''."""
        non_alphanumeric = ['!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<',
                            '=', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~']

        random_length = self.fake.random_int(min=26, max=100)  # Random length between 26 and 100
        random_string = ''.join(
            self.fake.random_element(non_alphanumeric + [self.fake.random_letter(), str(self.fake.random_digit())])
            for _ in range(random_length)
        )

        filtered_result = self.genai_data_wrangler.filter_long_text(random_string)

        assert len(filtered_result) == 0

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_long_text_short_text(self, iteration):
        """Test that filter_long_text() does not filter out short text."""
        non_alphanumeric = ['!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<',
                            '=', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~']

        random_length = self.fake.random_int(min=1, max=25)  # Random length between 26 and 100
        random_string = ''.join(
            self.fake.random_element(non_alphanumeric + [self.fake.random_letter(), str(self.fake.random_digit())])
            for _ in range(random_length)
        )

        filtered_result = self.genai_data_wrangler.filter_long_text(random_string)

        assert len(random_string) == len(filtered_result)

    @pytest.mark.parametrize("iteration", range(10))
    def test_process_messages_for_genai(self, fake_message_df, fake_recipient_df, iteration):
        """Test that process_messages_for_genai() returns a DataFrame with the correct columns."""
        processed_messages = self.genai_data_wrangler.process_messages_for_genai(fake_message_df, fake_recipient_df,
                                                                                 self.thread_id_n)

        assert isinstance(processed_messages, pd.DataFrame)
        assert set(processed_messages.columns) == {"date_sent_datetime", "message_author", "body",
                                                   "quote_id_datetime", "quote_author", "quote_body"}

    @pytest.mark.parametrize("iteration", range(10))
    def test_message_data_to_json(self, fake_processed_message_df, iteration):
        """Test that message_data_to_json() returns a JSON string."""
        json_string = self.genai_data_wrangler.message_data_to_json(fake_processed_message_df)

        assert isinstance(json_string, str)
        assert isinstance(json.loads(json_string), dict)

    @pytest.mark.parametrize("iteration", range(10))
    def test_process_user_data_for_genai(self, fake_recipient_df, iteration):
        """Test that process_user_data_for_genai() returns a DataFrame with the correct columns."""
        processed_user_df = self.genai_data_wrangler.process_user_data_for_genai(fake_recipient_df)

        assert isinstance(processed_user_df, pd.DataFrame)
        assert set(processed_user_df.columns) == {"profile_joined_name", "phone"}

    @pytest.mark.parametrize("iteration", range(10))
    def test_user_data_to_json(self, fake_processed_user_df, iteration):
        """Test that message_data_to_json() returns a JSON string."""
        json_string = self.genai_data_wrangler.user_data_to_json(fake_processed_user_df)

        assert isinstance(json_string, str)
        assert isinstance(json.loads(json_string), dict)