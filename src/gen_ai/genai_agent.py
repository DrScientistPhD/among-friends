from dotenv import find_dotenv, load_dotenv

from src.gen_ai.agent_tools import AgentTools, ToolBuilder
from src.gen_ai.genai_assets import (
    LLMBuilder,
    MemoryBuilder,
    PromptBuilder,
)

from src.gen_ai.genai_agent_builders import AgentChainBuilder

_ = load_dotenv(find_dotenv())

llm = LLMBuilder().get_llm()
memory = MemoryBuilder().initialize_conversational_memory()

agent_tools = AgentTools(llm=llm)

tool_builder = ToolBuilder(agent_tools, llm)

tools = tool_builder.build()

prompt = PromptBuilder().build()

agent_chain_builder = AgentChainBuilder(llm, tools, prompt, memory)
agent_chain = agent_chain_builder.build()


agent_chain.invoke({"input": "Give me an example of someone talking about cats."})
agent_chain.invoke(
    {"input": "Tell me something the group chat talked about in May 2023."}
)
agent_chain.invoke({"input": "Does Holly have children?"})
agent_chain.invoke({"input": "Be more specific, please."})


