import networkx as nx
import pandas as pd
import pytest
from faker import Faker

fake = Faker()


@pytest.fixture
def fake_message_df():
    """
    Fixture to generate fake DataFrame data for testing messages DataFrames.

    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """

    n = 100

    ids_1 = [fake.random_int(min=1, max=1000) for _ in range(n)]

    ids_2 = [fake.random_int(min=1, max=1000) for _ in range(n)]

    date_sent = [
        fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
    ]

    return pd.DataFrame(
        {
            "_id": ids_1,
            "date_sent": date_sent,
            "thread_id": [fake.random_int(min=1, max=3) for _ in range(n)],
            "from_recipient_id": [fake.random_int(min=1, max=3) for _ in range(n)],
            "body": [fake.sentence(nb_words=6) for _ in range(n)],
            "quote_id": ids_2,  # Using ids as quote_id to mimic reference to another message
            "quote_author": [fake.random_int(min=1, max=3) for _ in range(n)],
            "quote_body": [fake.sentence(nb_words=6) for _ in range(n)],
        }
    )


@pytest.fixture
def fake_recipient_df():
    """
    Fixture to generate fake DataFrame data for testing recipient DataFrames.

    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """

    n = 100

    return pd.DataFrame(
        {
            "_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "phone": [fake.random_int(min=1111111111, max=9999999999) for _ in range(n)],
            "system_display_name": [fake.name() for _ in range(n)],
            "profile_joined_name": [fake.name() for _ in range(n)],
        }
    )


@pytest.fixture
def fake_emoji_df():
    """
    Fixture to generate fake DataFrame data for testing emojis DataFrame.

    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """

    n = 100

    date_sent = [
        fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
    ]

    return pd.DataFrame(
        {
            "_id": [fake.random_int() for _ in range(n)],
            "message_id": [fake.random_int() for _ in range(n)],
            "author_id": [fake.random_int() for _ in range(n)],
            "emoji": [
                fake.random_element(elements=("😍", "🐍", "❤️", "👍", "🙌"))
                for _ in range(n)
            ],
            "date_sent": date_sent,
        }
    )


@pytest.fixture
def fake_message_slim_df():
    """
    Fixture to generate fake DataFrame data that represents the structure of a DataFrame
    after being processed by the filter_and_rename_messages_df function.

    Returns:
        pd.DataFrame: Fake DataFrame with the expected columns.
    """

    n = 100

    ids = [fake.random_int(min=1, max=1000) for _ in range(n)]

    comment_date_sent = [
        fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
    ]

    return pd.DataFrame(
        {
            "comment_id": ids,
            "comment_date_sent": comment_date_sent,
            "comment_thread_id": [fake.random_int(min=1, max=3) for _ in range(n)],
            "comment_from_recipient_id": [
                fake.random_int(min=1, max=1000) for _ in range(n)
            ],
            "comment_body": [fake.sentence(nb_words=6) for _ in range(n)],
            "quote_id": ids,  # Using ids as quote_id to mimic reference to another message
        }
    )


@pytest.fixture
def fake_emoji_slim_df():
    """
    Fixture to generate fake DataFrame data for testing a slim emojis DataFrame.

    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """

    n = 100

    date_sent = [
        fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
    ]

    # TODO: This can be improved by better and more consistent randomized data
    return pd.DataFrame(
        {
            "emoji_id": [fake.random_int() for _ in range(n)],
            "message_id": [fake.random_int() for _ in range(n)],
            "emoji_author_id": [fake.random_int() for _ in range(n)],
            "emoji": [
                fake.random_element(elements=("😍", "🐍", "❤️", "👍", "🙌"))
                for _ in range(n)
            ],
            "emoji_date_sent": date_sent,
            "emoji_translation": [
                fake.random_element(
                    elements=("heart_face", "snake", "heart", "thumbs_up", "raise")
                )
                for _ in range(n)
            ],
        }
    )


@pytest.fixture
def fake_quotation_response_df():
    """
    Fixture to generate fake DataFrame data for testing quotation-response DataFrame.

    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """
    n = 100

    # Generate 'quotation_date_sent' and 'response_date_sent' first
    quotation_date_sent = [
        fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
    ]
    response_date_sent = [
        fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
    ]

    # Calculate the time difference in seconds and store in 'time_diff'
    time_diff = [
        (start - end) / 1000
        for start, end in zip(quotation_date_sent, response_date_sent)
    ]

    data = {
        "quotation_id": fake.random_elements(
            elements=range(1000), length=n, unique=True
        ),
        "quotation_date_sent": quotation_date_sent,
        "quotation_thread_id": fake.random_elements(
            elements=range(1000), length=n, unique=False
        ),
        "quotation_from_recipient_id": fake.random_elements(
            elements=range(1000), length=n, unique=False
        ),
        "quotation_body": fake.sentences(nb=n, ext_word_list=None),
        "response_id": fake.random_elements(
            elements=range(1000), length=n, unique=True
        ),
        "response_date_sent": response_date_sent,
        "response_thread_id": fake.random_elements(
            elements=range(1000), length=n, unique=False
        ),
        "response_from_recipient_id": fake.random_elements(
            elements=range(1000), length=n, unique=False
        ),
        "response_body": fake.sentences(nb=n, ext_word_list=None),
        "quote_id": fake.random_elements(elements=range(1000), length=n, unique=False),
        "time_diff": time_diff,
    }

    return pd.DataFrame(data)


@pytest.fixture
def fake_response_react_dataframe():
    """
    Returns a sample DataFrame with randomly generated data for comment-response pairs.
    """

    n = 100

    data = {
        "comment_from_recipient_id": [
            fake.random_int(min=1, max=100) for _ in range(n)
        ],
        "comment_date_sent_datetime": [fake.date_this_decade() for _ in range(n)],
        "response_from_recipient_id": [
            fake.random_int(min=1, max=100) for _ in range(n)
        ],
        "response_date_sent_datetime": [fake.date_this_decade() for _ in range(n)],
        "weight": [fake.random_number(digits=2) for _ in range(n)],
        "interaction_category": "response",
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_emoji_react_dataframe():
    """
    Returns a sample DataFrame with randomly generated data for comment-emoji pairs.
    """

    n = 100

    data = {
        "comment_from_recipient_id": [
            fake.random_int(min=1, max=100) for _ in range(n)
        ],
        "comment_date_sent_datetime": [fake.date_this_decade() for _ in range(n)],
        "emoji_author_id": [fake.random_int(min=1, max=100) for _ in range(n)],
        "emoji_date_sent_datetime": [fake.date_this_decade() for _ in range(n)],
        "weight": [fake.random_number(digits=2) for _ in range(n)],
        "interaction_category": "emoji",
    }
    return pd.DataFrame(data)


@pytest.fixture
def fake_quotation_react_dataframe():
    """
    Returns a sample DataFrame with randomly generated data for quotation-response pairs.
    """

    n = 100

    data = {
        "quotation_from_recipient_id": [
            fake.random_int(min=1, max=100) for _ in range(n)
        ],
        "quotation_date_sent_datetime": [fake.date_this_decade() for _ in range(n)],
        "response_from_recipient_id": [
            fake.random_int(min=1, max=100) for _ in range(n)
        ],
        "response_date_sent_datetime": [fake.date_this_decade() for _ in range(n)],
        "weight": [fake.random_number(digits=2) for _ in range(n)],
        "interaction_category": "quotation",
    }
    return pd.DataFrame(data)


@pytest.fixture
def fake_nodes_edges_dataframe():
    """
    Returns a sample DataFrame with randomly generated data for the nodes_edges dataframe.
    """

    n = 100

    data = {
        "target_participant_id": [fake.random_int(min=1, max=100) for _ in range(n)],
        "target_datetime": [fake.date_this_decade() for _ in range(n)],
        "source_participant_id": [fake.random_int(min=1, max=100) for _ in range(n)],
        "source_datetime": [fake.date_this_decade() for _ in range(n)],
        "weight": [fake.random_number(digits=2) for _ in range(n)],
        "interaction_category": [
            fake.random_element(elements=("response", "quotation", "emoji"))
            for _ in range(n)
        ],
    }
    return pd.DataFrame(data)


@pytest.fixture
def fake_network_graph():
    """
    Returns a sample directed graph (nx.DiGraph) with randomly generated nodes and edges.
    """

    g = nx.DiGraph()

    # Add nodes
    node_ids = set()  # To track the unique node IDs
    while len(node_ids) < 10:
        node_ids.add(fake.random_int(min=1, max=100))
    for node_id in node_ids:
        g.add_node(node_id)

    # Add edges with weights and interaction_category
    for _ in range(15):
        source, target = fake.random_sample(elements=list(g.nodes()), length=2)
        g.add_edge(
            source,
            target,
            weight=fake.random_number(digits=2),
            interaction_category=fake.random_element(
                elements=("response", "quotation", "emoji")
            ),
        )

    return g


@pytest.fixture
def fake_processed_message_df():
    """
    Fixture to generate fake DataFrame data for testing processed messages for GenAI purposes.

    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """

    n = 100

    return pd.DataFrame(
        {
            "date_sent_string_date": [fake.date_time().strftime("%B %d, %Y %I:%M %p") for _ in range(n)],
            "message_author": [fake.name() for _ in range(n)],
            "body": [fake.sentence(nb_words=6) for _ in range(n)],
            "quote_id_string_date": [fake.date_time().strftime("%B %d, %Y %I:%M %p") for _ in range(n)],
            "quote_author": [fake.name() for _ in range(n)],
            "quote_body": [fake.sentence(nb_words=6) for _ in range(n)],
        }
    )


@pytest.fixture
def fake_list_of_sentences():
    """
    Fixture to generate a fake list of sentences

    Returns:
        List[str]: A list of randomly generated sentences.
    """

    n = 100

    return [fake.sentence(nb_words=fake.random_int(min=1, max=20)) for _ in range(n)]
