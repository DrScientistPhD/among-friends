import os
from datetime import datetime
from typing import Any, Optional, Union

import pinecone
from langchain.chains import LLMMathChain
from langchain.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from langchain.schema.vectorstore import VectorStoreRetriever
from langchain.tools import StructuredTool
from langchain.vectorstores import Pinecone
from langchain_openai import ChatOpenAI

from schema.pydantic_models import DocsWithFilterModel


class RetrieverBuilder:
    def __init__(self, api_key, environment, index_name, model_name):
        self.api_key = api_key
        self.environment = environment
        self.index_name = index_name
        self.model_name = model_name

    def build(self, query, search_filter=None):
        try:
            pinecone.init(
                api_key=self.api_key,
                environment=self.environment,
            )
            index = pinecone.Index(index_name=self.index_name)
            embed = SentenceTransformerEmbeddings(model_name=self.model_name)
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

        except Exception as e:
            return []


class AgentTools:
    def __init__(self, llm: ChatOpenAI):
        self.llm = llm
        self.retriever_builder = RetrieverBuilder(
            api_key=os.environ["PINECONE_API_KEY"],
            environment=os.environ["PINECONE_ENV"],
            index_name="among-friends",
            model_name="all-distilroberta-v1",
        )

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

    def get_docs_with_filter(
        self,
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
        return self.retriever_builder.build(query, search_filter)

    def solve_math_problem(self, question: dict[str, Any]):
        llm_math = LLMMathChain.from_llm(llm=self.llm)
        return llm_math.invoke(question)

    @staticmethod
    def solve_math_problem_static(question: dict[str, Any], llm: ChatOpenAI):
        instance = AgentTools(llm=llm)
        return instance.solve_math_problem(question)

    @staticmethod
    def get_date_and_local_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class ToolFactory:
    def __init__(self, agent_tools, llm):
        self.agent_tools = agent_tools
        self.llm = llm

    def get_docs_with_filter_tool(self):
        return StructuredTool.from_function(
            func=self.agent_tools.get_docs_with_filter,
            name="DocsRetrieverWithFilterTool",
            description="Use this function to generate the search_filter argument and query the vector database.",
            args_schema=DocsWithFilterModel,
            return_direct=False,
        )

    def math_tool(self):
        return StructuredTool.from_function(
            func=lambda question: self.agent_tools.solve_math_problem_static(
                question, self.llm
            ),
            name="CalculatorTool",
            description="Useful for when you need to answer questions about math.",
            return_direct=False,
        )

    def get_date_and_local_time_tool(self):
        return StructuredTool.from_function(
            func=self.agent_tools.get_date_and_local_time,
            name="GetDateAndLocalTime",
            description="Use when you need to get the current date and time.",
            return_direct=False,
        )


class ToolBuilder:
    def __init__(self, agent_tools, llm):
        self.tool_factory = ToolFactory(agent_tools, llm)

    def build(self):
        tools = []
        for method_name in dir(self.tool_factory):
            if method_name.endswith("_tool"):
                method = getattr(self.tool_factory, method_name)
                tool = method()
                tools.append(tool)
        return tools
