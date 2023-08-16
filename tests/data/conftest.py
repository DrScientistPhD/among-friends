import pandas as pd
import pytest
from faker import Faker

fake = Faker()


@pytest.fixture
def fake_message_df():
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
            "thread_id": [fake.random_int(min=1, max=3) for _ in range(n)],
            "from_recipient_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "body": [fake.sentence(nb_words=6) for _ in range(n)],
            "quote_id": ids,  # Using ids as quote_id to mimic reference to another message
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
                fake.random_element(elements=("heart_face", "snake", "heart", "thumbs_up", "raise"))
                for _ in range(n)
            ]
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
def fake_response_weight_df():
    """
    Fixture to generate fake DataFrame data that represents the structure of any response weight DataFrame. Note, this
    includes all possible columns from any dataframe the function takes.

    Returns:
        pd.DataFrame: Fake DataFrame with the expected columns.
    """

    n = 100

    ids = [fake.random_int(min=1, max=1000) for _ in range(n)]
    date_sent = [fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)]
    body = [fake.sentence(nb_words=6) for _ in range(n)]
    time_diff = [fake.random_int(min=0, max=10000) for _ in range(n)]
    weight = [fake.random_number(digits=2) for _ in range(n)]

    return pd.DataFrame(
        {
            "comment_id": ids,
            "comment_thread_id": [fake.random_int(min=1, max=3) for _ in range(n)],
            "comment_from_recipient_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "quote_id": ids,
            "comment_date_sent": date_sent,
            "comment_body": body,
            "response_id": ids,
            "response_date_sent": date_sent,
            "response_from_recipient_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "response_body": body,
            "time_diff": time_diff,
            "weight": weight,
            "comment_date_sent_datetime": [fake.date_time() for _ in range(n)],
            "response_date_sent_datetime": [fake.date_time() for _ in range(n)],
            "reaction_author_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "reaction_date_sent_datetime": [fake.date_time() for _ in range(n)],
            "quotation_from_recipient_id": [fake.random_int(min=1, max=1000) for _ in range(n)],
            "quotation_date_sent_datetime": [fake.date_time() for _ in range(n)]
        }
    )