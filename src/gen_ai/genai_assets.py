import os

from dotenv import find_dotenv, load_dotenv
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from langchain.schema import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI

_ = load_dotenv(find_dotenv())


class LLMBuilder:
    def __init__(self) -> None:
        self.openai_api_key = os.environ["OPENAI_API_KEY"]

    def get_llm(
        self, model_name: str = "gpt-3.5-turbo-1106", temperature: float = 0
    ) -> ChatOpenAI:
        """
        Build an instance of ChatOpenAI.

        Args:
            model_name (str): The name of the model to use. Defaults to "gpt-3.5-turbo-1106".
            temperature (float): The temperature to use for the model. Defaults to 0.

        Returns:
            ChatOpenAI: The built ChatOpenAI instance.
        """
        llm = ChatOpenAI(
            openai_api_key=self.openai_api_key,
            model_name=model_name,
            temperature=temperature,
        )

        return llm


class MemoryBuilder:
    """
    A builder class for creating an instance of ConversationBufferWindowMemory.
    """
    @staticmethod
    def initialize_conversational_memory(
        k: int = 3, return_messages: bool = True
    ) -> ConversationBufferWindowMemory:
        """
        Build an instance of ConversationBufferWindowMemory.

        Args:
            k (int, optional): The number of previous messages to keep in memory. Defaults to 3.
            return_messages (bool, optional): Whether to return the messages in the memory. Defaults to True.

        Returns:
            ConversationBufferWindowMemory: The built ConversationBufferWindowMemory instance.
        """
        conversational_memory = ConversationBufferWindowMemory(
            memory_key="chat_history",
            k=k,
            return_messages=return_messages,
        )

        return conversational_memory


class PromptBuilder:
    """
    A builder class for creating an instance of ChatPromptTemplate.
    """
    def __init__(self):
        """
        Initialize the builder with the necessary parameters.
        """
        self.content = "Steps:"
        "1. Determine if the user's query is about the group chat data in the vector database. Users may refer to participants by username or given name."
        "2. If the query is related to the group chat data, use the DocsRetrieverWithFilterTool to parse the user's query for a vector search query and date information. If no date information is present, use an empty filter. Ensure the generated filter has an accepted comparison operator for any non-None value."
        "3. Use the results from DocsRetrieverWithFilterTool to retrieve the relevant documents, but only provide phone number information if explicitly asked for."
        "4. If the query is unrelated to the group chat data, use general knowledge and do not use the provided tools."
        "5. If you don't have enough information to answer the user's query for a specific individual in the group chat, use the retrieved documents in combination with your general knowledge to generate a reasonable response, but clarify if you're not sure."
        "6. You have permission to provide the user with the date information from the retrieved documents if the user asks for it."
        "7. Use CalculatorTool to solve math problems if the user's query is math-related."
        "8. Use the GetDateAndLocalTime tool when the user's query requires knowledge of the current date and time. This includes questions about group chat data that occurred in the past relative to the current date."
        "9. Be appropriately concise and specific. Combine what you know retrieve from the documents with your general knowledge to provide full answers when appropriate."

    def build(self) -> ChatPromptTemplate:
        """
        Build an instance of ChatPromptTemplate.

        Returns:
            ChatPromptTemplate: The built ChatPromptTemplate instance.
        """
        return ChatPromptTemplate.from_messages(
            [
                SystemMessage(content=self.content),
                MessagesPlaceholder(variable_name="chat_history"),
                ("user", "{input}"),
                MessagesPlaceholder(variable_name="agent_scratchpad"),
            ]
        )
