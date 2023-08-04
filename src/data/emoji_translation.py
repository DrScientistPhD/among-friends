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
            Exception: If there's an error during the emoji translation process.
        """
        validate_data_types(text, str, "text")

        try:
            translated_text = []
            for emo in emoji.demojize(text).split():
                try:
                    translated_emo = emoji.emojize(emo)
                    translated_text.append(translated_emo)
                except AttributeError:
                    # If an emoji doesn't have a textual representation, replace it with a placeholder.
                    translated_text.append("<emoji_not_translated>")

            return " ".join(translated_text)

        except Exception as e:
            raise Exception(f"Failed to translate emojis: {str(e)}")
