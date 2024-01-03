from dotenv import find_dotenv, load_dotenv

_ = load_dotenv(find_dotenv())

import os

import pinecone
from dotenv import find_dotenv, load_dotenv

from src.data.data_mover import CSVMover, DocumentMover, TextMover
from src.data.genai_preparation import ProcessMessageData, ProcessUserData
from src.data.manage_vectordb import ManageVectorDb
from src.data.recipient_mapper import RecipientMapper
_ = load_dotenv(find_dotenv())
pinecone_api_key = os.environ["PINECONE_API_KEY"]
pinecone_env = os.environ["PINECONE_ENV"]
openai_api_key = os.environ["OPENAI_API_KEY"]

message = CSVMover.import_csv("data/production_data/raw", "message")
recipient = CSVMover.import_csv("data/production_data/raw", "recipient")




thread_id = 2

mapper = RecipientMapper(default_author_name="Raymond Pasek")

process_message_data = ProcessMessageData(recipient_mapper_instance=mapper)

processed_message_df = process_message_data.clean_up_messages(
    message, recipient, thread_id
)

message_sentences_df = ProcessMessageData.processed_message_data_to_sentences(
    processed_message_df
)

messages_data_txt = ProcessMessageData.concatenate_with_neighbors(message_sentences_df)

user_data_txt = ProcessUserData.user_data_to_sentences(recipient)





TextMover.export_sentences_to_file(
    messages_data_txt, "data/production_data/processed", "message_data"
)
TextMover.export_sentences_to_file(
    user_data_txt, "data/production_data/processed", "user_data"
)


messages_docs = DocumentMover.load_and_split_text(
    "data/production_data/processed", "message_data"
)

user_docs = DocumentMover.load_and_split_text("data/production_data/processed", "user_data")

index_name = "among-friends"

ManageVectorDb.create_index(index_name)

ManageVectorDb.upsert_text_embeddings_to_pinecone(index_name, user_docs)
ManageVectorDb.upsert_text_embeddings_to_pinecone(index_name, messages_docs)



index = pinecone.Index(index_name=index_name)
existing_ids_list = list(ManageVectorDb.get_all_ids_from_index(index_name))
#
# # Deleting IDs in batches of 999
# for i in range(0, len(existing_ids_list), 999):
#     batch_ids = existing_ids_list[i:i + 999]
#     index.delete(ids=batch_ids)
