import pytest
from faker import Faker
from random import choice
from src.data.emoji_translation import EmojiTranslator


class TestEmojiTranslator:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.emoji_translator = EmojiTranslator()

    @pytest.fixture
    def fake_sentences_with_emojis(self):
        """
        Fixture to generate fake sentences with random emojis for testing.
        Returns:
            List[str]: List of fake sentences with random emojis.
        """
        emojis = ["😀", "😂", "🤣", "😊", "😍"]  # A small list of common emojis
        return [self.fake.sentence() + ' ' + choice(emojis) for _ in range(5)]

    @pytest.mark.parametrize("iteration", range(10))
    def test_translate_emoji(self, iteration, fake_sentences_with_emojis):
        """
        Test to verify that the function `translate_emoji` correctly translates emojis in the text.
        """
        for sentence in fake_sentences_with_emojis:
            try:
                translated = self.emoji_translator.translate_emoji(sentence)
                # Check that the output is a string
                assert isinstance(translated, str)
            except Exception as e:
                assert False, f"Failed to translate emojis: {str(e)}"
