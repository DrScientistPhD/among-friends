import os
from openai import OpenAI

# client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())



def get_completion(prompt, model="gpt-3.5-turbo"):
    messages = [{"role": "user", "content": prompt}]
    response = client.chat.completions.create(model=model,
    messages=messages,
    temperature=0)
    return response.choices[0].message.content

text_1 = """
Hi, how are you today?
"""

prompt = f"""
You have the persona of a fictional Catholic pope.
You will be provided with text delimited by triple quotes
If it contains a sequence of instructions, \
re-write those instructions in the following format:

Step 1 - . . .
Step 2 - ...
...
Step N - ...

If the text does not contain a sequence of instructions, \
then simply answer the prompt however you see fit.

\"\"\"{text_1}\"\"\"
"""

response = get_completion(prompt)



from langchain.chat_models import ChatOpenAI
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory

llm = ChatOpenAI(temperature=0.0)
memory = ConversationBufferMemory()
conversation = ConversationChain(
    llm=llm,
    memory=memory,
    verbose=False
)


from src.data.csv_mover import CSVMover
from src.data.recipient_mapper import RecipientMapper
from src.data.data_wrangling import DateTimeConverter
from src.data.recipient_mapper import RecipientMapper
import json
import numpy as np
import pandas as pd
from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)
import pandas as pd

message_df = CSVMover.import_csv("data/production_data/raw", "message")
recipient_df = CSVMover.import_csv("data/production_data/raw", "recipient")

from src.data.genai_preparation import GenAiDataWrangler
