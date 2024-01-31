import os
from typing import Optional, Union
from langchain.tools import DuckDuckGoSearchRun
import pinecone
from dotenv import find_dotenv, load_dotenv
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from langchain.docstore.document import Document
from langchain.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from langchain.schema import SystemMessage
from langchain.schema.vectorstore import VectorStoreRetriever
from langchain.tools import StructuredTool
from langchain.vectorstores import Pinecone
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI

from schema.pydantic_models import DocsWithFilterModel, ExtractMetadataModel

_ = load_dotenv(find_dotenv())


class MyChatOpenAI:
    def __init__(self) -> None:
        self.openai_api_key = os.environ["OPENAI_API_KEY"]

    # Create a function to define the ChatOpenAI LLM model
    def get_llm(self, model_name: str = "gpt-3.5-turbo-1106", temperature: float = 0):
        llm = ChatOpenAI(
            openai_api_key=self.openai_api_key,
            model_name=model_name,
            temperature=temperature,
        )

        return llm


class MyConversationBufferWindowMemory:
    def initialize_conversational_memory(
        self, k: int = 3, return_messages: bool = True
    ) -> ConversationBufferWindowMemory:
        conversational_memory = ConversationBufferWindowMemory(
            memory_key="chat_history",
            k=k,
            return_messages=return_messages,
        )

        return conversational_memory


class AgentTools:
    @staticmethod
    def get_search_filter(
        sent_day_operator: Optional[str] = None,
        sent_day: Optional[Union[int, list[int]]] = None,
        sent_day_of_week_operator: Optional[str] = None,
        sent_day_of_week: Optional[Union[str, list[str]]] = None,
        sent_month_operator: Optional[str] = None,
        sent_month: Optional[Union[str, list[str]]] = None,
        sent_year_operator: Optional[str] = None,
        sent_year: Optional[Union[int, list[int]]] = None,
    ):
        search_filter = {
            "filter": {
                "sent_day": {sent_day_operator: sent_day}
                if sent_day is not None
                else None,
                "sent_day_of_week": {sent_day_of_week_operator: sent_day_of_week}
                if sent_day_of_week is not None
                else None,
                "sent_month": {sent_month_operator: sent_month}
                if sent_month is not None
                else None,
                "sent_year": {sent_year_operator: sent_year}
                if sent_year is not None
                else None,
            }
        }
        search_filter["filter"] = {
            k: v for k, v in search_filter["filter"].items() if v is not None
        }

        return search_filter

    @staticmethod
    def doc_retriever(
        query: str, search_filter: Optional[dict] = None
    ) -> VectorStoreRetriever:
        pinecone.init(
            api_key=os.environ["PINECONE_API_KEY"],
            environment=os.environ["PINECONE_ENV"],
        )
        index = pinecone.Index(index_name="among-friends")
        embed = SentenceTransformerEmbeddings(model_name="all-distilroberta-v1")
        vectorstore = Pinecone(index, embed, "text")
        retriever_args = {"search_type": "mmr"}
        k = 5

        if search_filter is not None:
            search_filter_with_k = {**search_filter, "k": k}
            retriever_args["search_kwargs"] = search_filter_with_k
        else:
            retriever_args["search_kwargs"] = {"k": k}

        retriever = vectorstore.as_retriever(**retriever_args)

        docs = retriever.get_relevant_documents(query)

        return docs

    @staticmethod
    def get_docs_with_filter(
        query: str,
        sent_day_operator: Optional[str] = None,
        sent_day: Optional[Union[int, list[int]]] = None,
        sent_day_of_week_operator: Optional[str] = None,
        sent_day_of_week: Optional[Union[str, list[str]]] = None,
        sent_month_operator: Optional[str] = None,
        sent_month: Optional[Union[str, list[str]]] = None,
        sent_year_operator: Optional[str] = None,
        sent_year: Optional[Union[int, list[int]]] = None,
    ) -> VectorStoreRetriever:
        search_filter = AgentTools.get_search_filter(
            sent_day_operator,
            sent_day,
            sent_day_of_week_operator,
            sent_day_of_week,
            sent_month_operator,
            sent_month,
            sent_year_operator,
            sent_year,
        )
        return AgentTools.doc_retriever(query, search_filter)

    @staticmethod
    def extract_metadata(documents: list[Document]):
        extracted_metadata = []

        for document in documents:
            metadata = document.metadata
            extracted_metadata.append(
                {
                    "sent_day": metadata.get("sent_day"),
                    "sent_day_of_week": metadata.get("sent_day_of_week"),
                    "sent_month": metadata.get("sent_month"),
                    "sent_year": metadata.get("sent_year"),
                }
            )

        return extracted_metadata

    @staticmethod
    def extract_metadata(documents: list[Union[Document, dict]]) -> list[dict]:
        extracted_metadata = []

        for document in documents:
            extracted_metadata.append(
                {
                    "sent_day": document.get("sent_day"),
                    "sent_day_of_week": document.get("sent_day_of_week"),
                    "sent_month": document.get("sent_month"),
                    "sent_year": document.get("sent_year"),
                }
            )

        return extracted_metadata


get_docs_with_filter_tool = StructuredTool.from_function(
    func=AgentTools.get_docs_with_filter,
    name="DocsRetrieverWithFilterTool",
    description="Use this function to generate the search_filter argument and query the vector database.",
    args_schema=DocsWithFilterModel,
    return_direct=False,
)

extract_metadata_tool = StructuredTool.from_function(
    func=AgentTools.extract_metadata,
    name="ExtractMetadataTool",
    description="Use this function to extract metadata from a list of Document objects.",
    args_schema=ExtractMetadataModel,
    return_direct=False
)


llm = MyChatOpenAI().get_llm()
memory = MyConversationBufferWindowMemory().initialize_conversational_memory()


prompt = ChatPromptTemplate.from_messages(
    [
        SystemMessage(
            content="Steps:"
            "1. Determine if the user's query is about the group chat data in the vector database. Users may refer to participants by username or given name."
            "2. If the query is related to the group chat data, use the DocsRetrieverWithFilterTool to parse the user's query for a vector search query and date information. If no date information is present, use an empty filter. Ensure the generated filter has an accepted comparison operator for any non-None value."
            "3. Use the results from DocsRetrieverWithFilterTool to retrieve the relevant documents."
            "4. If the user's query is asking about when something was said, use the DocsRetrieverWithFilterTool and then the ExtractMetadataTool to parse date information from the retrieved documents to answer the user's query."
            "6. If the query is unrelated to the group chat data, use general knowledge and do not use the provided tools."
            "7. If you don't have enough information to answer the user's query for a specific individual in the group chat, use the retrieved documents in combination with your general knowledge to generate a reasonable response, but clarify if you're not sure."
            "7. Be appropriately concise."
        ),
        MessagesPlaceholder(variable_name="chat_history"),
        ("user", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]
)

tools = [get_docs_with_filter_tool, extract_metadata_tool]

agent = create_openai_tools_agent(
    llm=llm,
    tools=tools,
    prompt=prompt,
)

agent_chain = AgentExecutor(
    agent=agent, tools=tools, verbose=True, memory=memory
)

agent_chain.invoke({"input": "When did Ray talk about the Queen of Heaven lawn fete?"})
