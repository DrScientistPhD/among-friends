import os
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from langchain.schema import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

from src.gen_ai.genai_assets import LLMBuilder, MemoryBuilder, PromptBuilder


class TestLLMBuilder:
    """
    Test class for the LLMBuilder class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.llm_builder = LLMBuilder()
        self.fake = Faker()

    @pytest.mark.parametrize("iteration", range(10))
    @patch('src.gen_ai.genai_assets.ChatOpenAI')
    def test_get_llm(self, mock_chat_openai, iteration):
        """
        This test checks if the method correctly initializes a ChatOpenAI instance with the provided parameters.
        """
        # Test with default parameters
        self.llm_builder.get_llm()
        mock_chat_openai.assert_called_once_with(
            openai_api_key=os.environ["OPENAI_API_KEY"],
            model_name="gpt-3.5-turbo-1106",
            temperature=0,
        )

        # Test with custom parameters
        mock_chat_openai.reset_mock()
        model_name = self.fake.word()
        temperature = self.fake.random.uniform(0, 1)
        self.llm_builder.get_llm(model_name=model_name, temperature=temperature)
        mock_chat_openai.assert_called_once_with(
            openai_api_key=os.environ["OPENAI_API_KEY"],
            model_name=model_name,
            temperature=temperature,
        )


class TestMemoryBuilder:
    """
    Test class for the MemoryBuilder class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.memory_builder = MemoryBuilder()
        self.fake = Faker()

    @pytest.mark.parametrize("iteration", range(10))
    @patch.object(ConversationBufferWindowMemory, "__init__", return_value=None)
    def test_initialize_conversational_memory(self, mock_init, iteration):
        """
        This test checks if the method correctly initializes a ConversationBufferWindowMemory instance with the provided parameters.
        """
        # Test with default parameters
        self.memory_builder.initialize_conversational_memory()
        mock_init.assert_called_once_with(
            memory_key="chat_history",
            k=3,
            return_messages=True,
        )

        # Test with custom parameters
        mock_init.reset_mock()
        k = self.fake.random_int(min=1, max=10)
        return_messages = self.fake.boolean()
        self.memory_builder.initialize_conversational_memory(
            k=k, return_messages=return_messages
        )
        mock_init.assert_called_once_with(
            memory_key="chat_history",
            k=k,
            return_messages=return_messages,
        )


class TestPromptBuilder:
    """
    Test class for the PromptBuilder class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.prompt_builder = PromptBuilder()

    def test_build(self):
        """
        Test the build method of the PromptBuilder class.
        This test checks if the method correctly builds a ChatPromptTemplate instance.
        """
        # Call the build method
        result = self.prompt_builder.build()

        # Check the type of the result
        assert isinstance(result, ChatPromptTemplate)

        # Check the content of the result
        assert isinstance(result.messages[0], SystemMessage)
        assert result.messages[0].content == self.prompt_builder.content
        assert isinstance(result.messages[1], MessagesPlaceholder)
        assert result.messages[1].variable_name == "chat_history"
        assert isinstance(result.messages[3], MessagesPlaceholder)
        assert result.messages[3].variable_name == "agent_scratchpad"
