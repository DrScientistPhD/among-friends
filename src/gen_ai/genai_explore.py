from dotenv import find_dotenv, load_dotenv

_ = load_dotenv(find_dotenv())

import os

import pinecone
from dotenv import find_dotenv, load_dotenv

from src.data.data_mover import DocumentMover
from src.data.manage_vectordb import ManageVectorDb

_ = load_dotenv(find_dotenv())
pinecone_api_key = os.environ["PINECONE_API_KEY"]
pinecone_env = os.environ["PINECONE_ENV"]
openai_api_key = os.environ["OPENAI_API_KEY"]

