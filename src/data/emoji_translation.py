import emoji

from src.data.data_validation import validate_data_types


class EmojiTranslator:
    @staticmethod
    def translate_emoji(text: str) -> str:
        """
        Translates emojis in the input text to their textual representation.

        Args:
            text (str): Input text containing emojis.

        Returns:
            str: Text with emojis replaced by their textual representation.
                 Untranslatable emojis will be replaced by '<emoji_not_translated>'.

        Raises:
            TypeError: If text is not a string.
            Exception: If there's an error during the emoji translation process.
        """
        # Validate input data
        validate_data_types(text, str, "text")

        try:
            translated_text = []
            for char in text:
                translated_emo = emoji.demojize(char)
                # If the translation is successful, append the translated emoji; otherwise, append the original
                # character
                if translated_emo != char:
                    translated_text.append(translated_emo)
                else:
                    translated_text.append("<emoji_not_translated>")

            return " ".join(translated_text)

        except Exception as e:
            raise Exception(f"Failed to translate emojis: {str(e)}")
