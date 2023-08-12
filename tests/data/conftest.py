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

    n = 100

    ids = [fake.random_int(min=1, max=1000) for _ in range(n)]

    date_sent = [
        fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
    ]

    return pd.DataFrame(
        {
            "_id": ids,
            "date_sent": date_sent,
            "thread_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "from_recipient_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "body": [fake.sentence(nb_words=6) for _ in range(n)],
            "quote_id": ids,  # Using ids as quote_id to mimic reference to another message
        }
    )


@pytest.fixture
def fake_filtered_and_renamed_dataframe():
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
            "comment_thread_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "comment_from_recipient_id": [
                fake.random_int(min=1, max=1000) for _ in range(n)
            ],
            "comment_body": [fake.sentence(nb_words=6) for _ in range(n)],
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
        "quote_id": fake.random_elements(elements=range(1000), length=n, unique=False),
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
