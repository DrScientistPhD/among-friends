from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI


class AgentExecutorBuilder:
    """
    A builder class for creating an instance of AgentExecutor.
    """

    def __init__(
        self,
        llm: ChatOpenAI,
        tools: list,
        prompt: ChatPromptTemplate,
        memory: ConversationBufferWindowMemory,
        verbose: bool = True,
        max_iterations: int = 5,
        handle_parsing_errors: str = "Ask the user to clarify.",
        max_execution_time: int = 60,
    ):
        """
        Initialize the builder with the necessary parameters.

        Args:
            llm (ChatOpenAI): The language model to use.
            tools (list): The tools to use.
            prompt (ChatPromptTemplate): The prompt to use.
            memory (ConversationBufferWindowMemory): The memory to use.
            verbose (bool, optional): Whether to print verbose output. Defaults to True.
            max_iterations (int, optional): The maximum number of iterations. Defaults to 5.
            handle_parsing_errors (str, optional): The error handling strategy. Defaults to "Ask the user to clarify.".
            max_execution_time (int, optional): The maximum execution time in seconds. Defaults to 60.
        """
        self.llm = llm
        self.tools = tools
        self.prompt = prompt
        self.memory = memory
        self.verbose = verbose
        self.max_iterations = max_iterations
        self.handle_parsing_errors = handle_parsing_errors
        self.max_execution_time = max_execution_time

    def build(self):
        agent = create_openai_tools_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=self.prompt,
        )

        return AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=self.verbose,
            memory=self.memory,
            max_iterations=self.max_iterations,
            handle_parsing_errors=self.handle_parsing_errors,
            max_execution_time=self.max_execution_time,
        )


class AgentChainBuilder:
    """
    A builder class for creating an instance of AgentExecutor using a chain of builders.
    """

    def __init__(
        self,
        llm: ChatOpenAI,
        tools: list,
        prompt: ChatPromptTemplate,
        memory: ConversationBufferWindowMemory,
    ):
        """
        Initialize the builder with the necessary parameters.

        Args:
            llm (ChatOpenAI): The language model to use.
            tools (list): The tools available for the LLM to use.
            prompt (ChatPromptTemplate): The prompt to provide for the LLM.
            memory (ConversationBufferWindowMemory): The memory object for the LLM to have access to previous messages.
        """
        self.executor_builder = AgentExecutorBuilder(
            llm=llm, tools=tools, prompt=prompt, memory=memory
        )

    def build(self):
        return self.executor_builder.build()
