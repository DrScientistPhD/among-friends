import pandas as pd
import pytest
from faker import Faker

fake = Faker()


@pytest.fixture
def fake_dataframe_messages():
    """
    Fixture to generate fake DataFrame data for testing messages DataFrame.
    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """
    ids = [fake.random_int(min=1, max=1000) for _ in range(100)]
    return pd.DataFrame(
        {
            "_id": ids,
            "date_sent": pd.date_range(start="1/1/2018", periods=100),
            "thread_id": [fake.random_int(min=1, max=1000) for _ in range(100)],
            "from_recipient_id": [
                fake.random_int(min=1, max=1000) for _ in range(100)
            ],
            "body": [fake.sentence(nb_words=6) for _ in range(100)],
            "quote_id": ids,  # Using ids as quote_id to mimic reference to another message
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
        "quote_id": fake.random_elements(
            elements=range(1000), length=n, unique=False
        ),
        "time_diff": time_diff,
    }

    return pd.DataFrame(data)


@pytest.fixture
def fake_dataframe_reactions():
    """
    Fixture to generate fake DataFrame data for testing reactions DataFrame.
    Returns:
        pd.DataFrame: Fake DataFrame with random data.
    """
    return pd.DataFrame(
        {
            "_id": [fake.random_int() for _ in range(10)],
            "message_id": [fake.random_int() for _ in range(10)],
            "author_id": [fake.random_int() for _ in range(10)],
            "emoji": [
                fake.random_element(elements=("😍", "🐍", "❤️", "👍", "🙌"))
                for _ in range(10)
            ],
            "date_sent": [fake.date_time() for _ in range(10)],
        }
    )
