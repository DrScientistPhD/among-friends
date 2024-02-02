import pytest
from faker import Faker
from unittest.mock import patch, MagicMock
from src.gen_ai.genai_agent_builders import AgentChainBuilder, AgentExecutorBuilder
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI


class TestAgentExecutorBuilder:
    """
    Test class for the AgentExecutorBuilder class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        self.fake = Faker()
        self.llm = MagicMock(spec=ChatOpenAI)
        self.tools = [MagicMock(), MagicMock()]
        self.prompt = MagicMock(spec=ChatPromptTemplate, input_variables={'agent_scratchpad'})
        self.memory = MagicMock(spec=ConversationBufferWindowMemory)
        self.builder = AgentExecutorBuilder(
            llm=self.llm,
            tools=self.tools,
            prompt=self.prompt,
            memory=self.memory,
        )

    @patch('src.gen_ai.genai_agent_builders.create_openai_tools_agent')
    @patch('src.gen_ai.genai_agent_builders.AgentExecutor')
    def test_build(self, mock_agent_executor, mock_create_openai_tools_agent):
        """
        Test to check if the 'build' method correctly creates an AgentExecutor instance.
        """
        mock_create_openai_tools_agent.return_value = MagicMock()
        mock_agent_executor.return_value = MagicMock()
        result = self.builder.build()
        mock_create_openai_tools_agent.assert_called_once_with(
            llm=self.llm,
            tools=self.tools,
            prompt=self.prompt,
        )
        mock_agent_executor.assert_called_once_with(
            agent=mock_create_openai_tools_agent.return_value,
            tools=self.tools,
            verbose=True,
            memory=self.memory,
            max_iterations=5,
            handle_parsing_errors="Ask the user to clarify.",
            max_execution_time=60,
        )
        assert isinstance(result, MagicMock)


class TestAgentChainBuilder:
    """
    Test class for the AgentChainBuilder class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        self.fake = Faker()
        self.llm = MagicMock(spec=ChatOpenAI)
        self.tools = [MagicMock(), MagicMock()]
        self.prompt = MagicMock(spec=ChatPromptTemplate, input_variables={'agent_scratchpad'})
        self.memory = MagicMock(spec=ConversationBufferWindowMemory)
        self.builder = AgentChainBuilder(
            llm=self.llm,
            tools=self.tools,
            prompt=self.prompt,
            memory=self.memory,
        )

    @patch.object(AgentExecutorBuilder, 'build')
    def test_build(self, mock_build):
        """
        Test to check if the 'build' method correctly creates an AgentExecutor instance.
        """

        mock_build.return_value = MagicMock()
        result = self.builder.build()
        mock_build.assert_called_once()
        assert isinstance(result, MagicMock)