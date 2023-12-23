import pandas as pd
import pytest
from faker import Faker

from src.data.genai_preparation import ProcessMessageData, ProcessUserData


class TestProcessMessageData:
    """Test class for the ProcessMessageData class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker("en_US")
        self.process_message_data = ProcessMessageData()
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

        filtered_result = self.process_message_data.filter_long_text(random_string)

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

        filtered_result = self.process_message_data.filter_long_text(random_string)

        assert len(random_string) == len(filtered_result)

    @pytest.mark.parametrize("iteration", range(10))
    def test_clean_up_messages(self, fake_message_df, fake_recipient_df, iteration):
        """Test that process_messages_for_genai() returns a DataFrame with the correct columns."""
        processed_messages = self.process_message_data.clean_up_messages(fake_message_df, fake_recipient_df,
                                                                         self.thread_id_n)

        assert isinstance(processed_messages, pd.DataFrame)
        assert set(processed_messages.columns) == {"date_sent_string_date", "message_author", "body",
                                                   "quote_id_string_date", "quote_author", "quote_body"}

    @pytest.mark.parametrize("iteration", range(10))
    def test_processed_message_data_to_sentences(self, fake_processed_message_df, iteration):
        """Test that message_data_to_sentences() returns a DataFrame with the correct columns."""
        sentences = self.process_message_data.processed_message_data_to_sentences(fake_processed_message_df)

        assert "sentence" in sentences.columns
        assert isinstance(sentences, pd.DataFrame)


class TestProcessUserData:
    """Test class for the ProcessUserData class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker("en_US")
        self.process_user_data = ProcessUserData()


    @pytest.mark.parametrize("iteration", range(10))
    def test_parse_phone_number_good(self, iteration):
        """Test that parse_phone_number() returns a dict with a region key and a country key."""
        phone = str(self.fake.random_int(min=1111111111, max=9999999999))

        assert isinstance(self.process_user_data.parse_phone_number(phone), dict)

    @pytest.mark.parametrize("iteration", range(10))
    def test_parse_phone_number_bad(self, iteration):
        """Test that parse_phone_number() raises a ValueError when passed an invalid phone number."""
        phone = self.fake.word()

        with pytest.raises(ValueError):
            self.process_user_data.parse_phone_number(phone)

    @pytest.mark.parametrize("iteration", range(10))
    def test_user_data_to_sentences(self, fake_recipient_df, iteration):
        """Test that message_data_to_sentences() returns a list with the correct number of elements."""
        sentences = self.process_user_data.user_data_to_sentences(fake_recipient_df)

        assert isinstance(sentences, list)
        assert len(fake_recipient_df) == len(sentences)
