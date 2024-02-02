from langchain.agents import AgentExecutor, create_openai_tools_agent


class AgentExecutorBuilder:
    def __init__(
        self,
        llm,
        tools,
        prompt,
        memory,
        verbose=True,
        max_iterations=5,
        handle_parsing_errors="Ask the user to clarify.",
        max_execution_time=60,
    ):
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
    def __init__(self, llm, tools, prompt, memory):
        self.executor_builder = AgentExecutorBuilder(
            llm=llm,
            tools=tools,
            prompt=prompt,
            memory=memory
        )

    def build(self):
        return self.executor_builder.build()