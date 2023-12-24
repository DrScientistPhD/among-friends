from dotenv import find_dotenv, load_dotenv

_ = load_dotenv(find_dotenv())

import os

import pinecone
from dotenv import find_dotenv, load_dotenv
from langchain.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from langchain.vectorstores import Pinecone

from src.data.data_mover import CSVMover, DocumentMover, TextMover
from src.data.genai_preparation import ProcessMessageData, ProcessUserData

_ = load_dotenv(find_dotenv())
pinecone_api_key = os.environ["PINECONE_API_KEY"]
pinecone_env = os.environ["PINECONE_ENV"]
openai_api_key = os.environ["OPENAI_API_KEY"]

message = CSVMover.import_csv("data/production_data/raw", "message")
recipient = CSVMover.import_csv("data/production_data/raw", "recipient")

thread_id = 2

processed_message_df = ProcessMessageData.clean_up_messages(
    message, recipient, thread_id
)

message_sentences_df = ProcessMessageData.processed_message_data_to_sentences(
    processed_message_df
)

messages_data_txt = ProcessMessageData.concatenate_with_neighbors(message_sentences_df)

user_data_txt = ProcessUserData.user_data_to_sentences(recipient)


TextMover.export_sentences_to_file(
    messages_data_txt, "data/production_data/processed", "messages_data"
)
TextMover.export_sentences_to_file(
    user_data_txt, "data/production_data/processed", "users_data"
)


messages_docs = DocumentMover.load_and_split_text(
    "data/production_data/processed", "messages_data"
)
docs = DocumentMover.load_and_split_text("data/production_data/processed", "users_data")






pinecone.init(api_key=pinecone_api_key, environment=pinecone_env)

index_name = "among-friends"

pinecone.list_indexes()
pinecone.create_index(name=index_name, metric="cosine", shards=1, dimension=768)
pinecone.list_indexes()

index = pinecone.Index(index_name)

model_name = "all-distilroberta-v1"

embeddings = SentenceTransformerEmbeddings(model_name=model_name)

docsearch = Pinecone.from_documents(docs, embeddings, index_name=index_name)

query = "Where does J.P. live?"
docs = docsearch.similarity_search(query, k=3)
docs

