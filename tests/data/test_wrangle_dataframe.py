import itertools

import numpy as np
import pandas as pd
import pytest
from faker import Faker

from src.data.wrangle_dataframe import DataFrameWrangler, RecipientMapper


class TestRecipientMapper:
    """
    Test class for the RecipientMapper class.
    """

    @pytest.fixture
    def fake_recipient_data(self):
        """
        Fixture to generate fake recipient data as a pandas DataFrame for testing.
        Returns:
            pd.DataFrame: Fake recipient data.
        """
        fake = Faker()
        data = {
            "_id": [fake.random_int(1, 100) for _ in range(10)],
            "profile_joined_name": [fake.name() for _ in range(10)],
        }
        return pd.DataFrame(data)

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.recipient_mapper = RecipientMapper()

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping(self, iteration, fake_recipient_data):
        """
        Test to check if the function returns the correct recipient_id to system_given_name dictionary.
        """
        # Calling the function with the fake recipient data.
        recipient_id_to_name = (
            self.recipient_mapper.create_recipient_id_to_name_mapping(
                fake_recipient_data
            )
        )

        # Ensure the recipient_id_to_name dictionary is correct.
        expected_dict = {
            row["_id"]: row["profile_joined_name"]
            for _, row in fake_recipient_data.iterrows()
        }
        assert recipient_id_to_name == expected_dict

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_empty_dataframe(self, iteration):
        """
        Test to check if the function raises a ValueError when an empty DataFrame is provided.
        """
        empty_df = pd.DataFrame()  # Create an empty DataFrame
        with pytest.raises(
            ValueError,
            match="df_recipient is missing required columns '_id' and/or 'profile_joined_name'",
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(empty_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_invalid_dataframe(self, iteration):
        """
        Test to check if the function raises a ValueError when an invalid DataFrame is provided.
        """
        # Create an invalid DataFrame missing the '_id' column
        invalid_df = pd.DataFrame({"profile_joined_name": ["Alice", "Bob", "Charlie"]})
        with pytest.raises(
            ValueError,
            match="df_recipient is missing required columns '_id' and/or 'profile_joined_name'",
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(invalid_df)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_recipient_id_to_name_mapping_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError or TypeError when invalid input is provided.
        """
        # Passing None as input
        with pytest.raises(TypeError, match="df_recipient must be a pandas DataFrame"):
            self.recipient_mapper.create_recipient_id_to_name_mapping(None)

        # Invalid column names in the DataFrame for this test case
        with pytest.raises(
            ValueError,
            match="df_recipient is missing required columns '_id' and/or 'profile_joined_name'",
        ):
            self.recipient_mapper.create_recipient_id_to_name_mapping(
                pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
            )


class TestDataFrameWrangler:
    """
    Test class for the DataFrameWrangler class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.dataframe_wrangler = DataFrameWrangler()

    @pytest.fixture
    def fake_dataframe_messages(self):
        """
        Fixture to generate fake DataFrame data for testing messages DataFrame.
        Returns:
            pd.DataFrame: Fake DataFrame with random data.
        """
        ids = [self.fake.random_int(min=1, max=1000) for _ in range(100)]
        return pd.DataFrame(
            {
                "_id": ids,
                "date_sent": pd.date_range(start="1/1/2018", periods=100),
                "thread_id": [
                    self.fake.random_int(min=1, max=1000) for _ in range(100)
                ],
                "from_recipient_id": [
                    self.fake.random_int(min=1, max=1000) for _ in range(100)
                ],
                "body": [self.fake.sentence(nb_words=6) for _ in range(100)],
                "quote_id": ids,  # Using ids as quote_id to mimic reference to another message
            }
        )

    @pytest.fixture
    def fake_quotation_response_df(self):
        """
        Fixture to generate fake DataFrame data for testing quotation-response DataFrame.
        Returns:
            pd.DataFrame: Fake DataFrame with random data.
        """
        n = 100

        # Generate 'quotation_date_sent' and 'response_date_sent' first
        quotation_date_sent = [
            self.fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
        ]
        response_date_sent = [
            self.fake.random_int(min=1673137959372, max=1673378972279) for _ in range(n)
        ]

        # Calculate the time difference in seconds and store in 'time_diff'
        time_diff = [
            (start - end) / 1000
            for start, end in zip(quotation_date_sent, response_date_sent)
        ]

        data = {
            "quotation_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=True
            ),
            "quotation_date_sent": quotation_date_sent,
            "quotation_thread_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "quotation_from_recipient_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "quotation_body": self.fake.sentences(nb=n, ext_word_list=None),
            "response_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=True
            ),
            "response_date_sent": response_date_sent,
            "response_thread_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "response_from_recipient_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "response_body": self.fake.sentences(nb=n, ext_word_list=None),
            "quote_id": self.fake.random_elements(
                elements=range(1000), length=n, unique=False
            ),
            "time_diff": time_diff,
        }

        return pd.DataFrame(data)

    @pytest.fixture
    def fake_dataframe_reactions(self):
        """
        Fixture to generate fake DataFrame data for testing reactions DataFrame.
        Returns:
            pd.DataFrame: Fake DataFrame with random data.
        """
        return pd.DataFrame(
            {
                "_id": [self.fake.random_int() for _ in range(10)],
                "message_id": [self.fake.random_int() for _ in range(10)],
                "author_id": [self.fake.random_int() for _ in range(10)],
                "emoji": [
                    self.fake.random_element(elements=("😍", "🐍", "❤️", "👍", "🙌"))
                    for _ in range(10)
                ],
                "date_sent": [self.fake.date_time() for _ in range(10)],
            }
        )

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_messages_df(self, iteration, fake_dataframe_messages):
        """
        Test to check if filter_and_rename_messages_df() filters and renames columns correctly.
        """
        # Rename columns in the fake DataFrame
        fake_dataframe_messages.rename(
            columns={
                "_id": "comment_id",
                "thread_id": "comment_thread_id",
                "from_recipient_id": "comment_from_recipient_id",
                "date_sent": "comment_date_sent",
                "body": "comment_body",
            },
            inplace=True,
        )

        # Set the expected column names after renaming
        expected_columns = [
            "comment_id",
            "comment_thread_id",
            "comment_from_recipient_id",
            "quote_id",
            "comment_date_sent",
            "comment_body",
        ]

        filtered_and_renamed_df = self.dataframe_wrangler.filter_and_rename_messages_df(
            fake_dataframe_messages
        )

        assert isinstance(filtered_and_renamed_df, pd.DataFrame)
        assert list(filtered_and_renamed_df.columns) == expected_columns

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_messages_df_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError when invalid input is provided.
        """
        # Passing a non-DataFrame input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_messages_df("invalid_data")

        # Passing None as input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_messages_df(None)

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_reactions_df(self, iteration, fake_dataframe_reactions):
        """
        Test to check if filter_and_rename_reactions_df() filters and renames columns correctly.
        """
        # Rename columns in the fake DataFrame
        fake_dataframe_reactions.rename(
            columns={
                "_id": "reaction_id",
                "author_id": "reaction_author_id",
                "date_sent": "reaction_date_sent",
            },
            inplace=True,
        )

        # Set the expected column names after renaming
        expected_columns = [
            "reaction_id",
            "message_id",
            "reaction_author_id",
            "emoji",
            "reaction_date_sent",
            "reaction_translation",
        ]

        filtered_and_renamed_df = (
            self.dataframe_wrangler.filter_and_rename_reactions_df(
                fake_dataframe_reactions
            )
        )

        assert isinstance(filtered_and_renamed_df, pd.DataFrame)
        assert list(filtered_and_renamed_df.columns) == expected_columns

    @pytest.mark.parametrize("iteration", range(10))
    def test_filter_and_rename_reactions_df_invalid_input(self, iteration):
        """
        Test to check if the function raises a ValueError when invalid input is provided.
        """
        # Passing a non-DataFrame input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_reactions_df("invalid_data")

        # Passing None as input
        with pytest.raises(ValueError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.filter_and_rename_reactions_df(None)

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_quotation_response_df_no_quotation(
        self, iteration, fake_dataframe_messages
    ):
        """
        Test to check if create_quotation_response_df() correctly handles the case where a message has no quotation.
        """
        # Modify the fake DataFrame so that the first entry has no quotation
        fake_dataframe_messages.loc[0, "quote_id"] = None

        quotation_response_df = DataFrameWrangler.create_quotation_response_df(
            fake_dataframe_messages
        )

        # If there are no quotations, the DataFrame should be empty
        assert quotation_response_df.empty

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_quotation_response_df_invalid_input(self, iteration):
        """
        Test to check if create_quotation_response_df() raises an error when given invalid input.
        """
        # Try calling create_quotation_response_df() with non-DataFrame inputs
        with pytest.raises(TypeError, match="dataframe must be a pandas DataFrame"):
            DataFrameWrangler.create_quotation_response_df("invalid_input")

    @pytest.mark.parametrize("iteration", range(10))
    def test_calculate_time_diff(self, iteration, fake_quotation_response_df):
        """
        Test to verify that the function `calculate_time_diff` correctly calculates the time difference
        between quotation and response date_sent and adds it as a new column to the DataFrame.
        """
        # Use the calculate_time_diff() function to calculate the time difference
        time_diff_df = self.dataframe_wrangler.calculate_time_diff(
            fake_quotation_response_df, "quotation_date_sent", "response_date_sent"
        )

        # Check that the output is a DataFrame
        assert isinstance(time_diff_df, pd.DataFrame)

        # Check that the 'time_diff' column exists in the output DataFrame
        assert "time_diff" in time_diff_df.columns

        # Check that a warning is raised when time_diff is negative
        with pytest.warns(
            UserWarning, match="There are negative time differences in the data"
        ):
            self.dataframe_wrangler.calculate_time_diff(
                fake_quotation_response_df, "quotation_date_sent", "response_date_sent"
            )

    @pytest.mark.parametrize("iteration", range(10))
    def test_calculate_time_diff_invalid_input(
        self, iteration, fake_quotation_response_df
    ):
        """
        Test to check if calculate_time_diff() raises an error when given invalid input.
        """
        # Try calling calculate_time_diff() with non-DataFrame inputs
        with pytest.raises(TypeError, match="dataframe must be a pandas DataFrame"):
            self.dataframe_wrangler.calculate_time_diff(
                "invalid_data", "quotation_date_sent", "response_date_sent"
            )

        # Try calling calculate_time_diff() with DataFrame but non-existing column names
        with pytest.raises(KeyError, match="Could not find column"):
            self.dataframe_wrangler.calculate_time_diff(
                fake_quotation_response_df,
                "non_existent_start_time",
                "non_existent_end_time",
            )

        # Check if the function correctly calculates time difference even when timestamps are equal
        fake_quotation_response_df_equal_timestamps = fake_quotation_response_df.copy()
        fake_quotation_response_df_equal_timestamps[
            "quotation_date_sent"
        ] = fake_quotation_response_df_equal_timestamps["response_date_sent"]

        result_df = self.dataframe_wrangler.calculate_time_diff(
            fake_quotation_response_df_equal_timestamps,
            "quotation_date_sent",
            "response_date_sent",
        )

        assert (
            result_df["time_diff"] == 0
        ).all(), "Time difference not correctly calculated for equal timestamps."

    @pytest.mark.parametrize(
        "iteration, percentile", itertools.product(range(10), [0.5, 0.75, 0.99])
    )
    def test_calculate_decay_constant(
        self, iteration, percentile, fake_quotation_response_df
    ):
        """
        Test to verify that the function `calculate_decay_constant` correctly calculates the decay constant based
        on the specified percentile of a specified column in the given DataFrame.
        """
        # Use the calculate_decay_constant() function to calculate the decay constant
        decay_constant = self.dataframe_wrangler.calculate_decay_constant(
            fake_quotation_response_df, "time_diff", percentile
        )

        # Check that the output is a float
        assert isinstance(decay_constant, float)

        # Try calling calculate_decay_constant() with DataFrame but non-existing column names
        with pytest.raises(KeyError, match="Could not find column"):
            self.dataframe_wrangler.calculate_decay_constant(
                fake_quotation_response_df, "non_existent_column", percentile
            )

        # Try calling calculate_decay_constant() with invalid percentile
        with pytest.raises(
            ValueError, match="Expected 'percentile' to be a float between 0 and 1"
        ):
            self.dataframe_wrangler.calculate_decay_constant(
                fake_quotation_response_df, "time_diff", -1
            )

        # Check the function with default percentile
        default_decay_constant = self.dataframe_wrangler.calculate_decay_constant(
            fake_quotation_response_df, "time_diff"
        )
        default_half_life = fake_quotation_response_df["time_diff"].quantile(0.75)
        assert abs(default_decay_constant - np.log(2) / default_half_life) < 1e-6

    @pytest.mark.parametrize("iteration", range(10))
    def test_calculate_weight(self, iteration, fake_quotation_response_df):
        """
        Test to verify that the function `calculate_weight` correctly calculates the weight using the exponential
        decay function and adds it as a new column to the DataFrame.
        """
        # Arbitrarily choose a decay constant for this test
        decay_constant = self.fake.random.uniform(0, 2)

        # Generate a random float for the base value using self.fake
        base_value = self.fake.random.uniform(0, 2)

        # Use the calculate_weight() function to add 'weight' column to the DataFrame
        df_with_weight = self.dataframe_wrangler.calculate_weight(
            fake_quotation_response_df, decay_constant, base_value
        )

        # Check that the 'weight' column has been added
        assert "weight" in df_with_weight.columns

        # Check that the 'weight' column values have been correctly calculated
        expected_weights = base_value * np.exp(
            -decay_constant * fake_quotation_response_df["time_diff"]
        )
        pd.testing.assert_series_equal(
            df_with_weight["weight"], expected_weights, check_names=False
        )

        # Try calling calculate_weight() with non-DataFrame, non-float decay_constant, and non-float base_value
        with pytest.raises(
            TypeError, match="Expected 'df' to be a pandas DataFrame, but got"
        ):
            self.dataframe_wrangler.calculate_weight(
                "not_a_dataframe", decay_constant, base_value
            )
        with pytest.raises(
            TypeError, match="Expected 'decay_constant' to be a float, but got"
        ):
            self.dataframe_wrangler.calculate_weight(
                fake_quotation_response_df, "not_a_float", base_value
            )
        with pytest.raises(
            TypeError, match="Expected 'base_value' to be a float, but got"
        ):
            self.dataframe_wrangler.calculate_weight(
                fake_quotation_response_df, decay_constant, "not_a_float"
            )

        # Try calling calculate_weight() with a DataFrame without 'time_diff' column
        df_without_time_diff = fake_quotation_response_df.drop(columns=["time_diff"])
        with pytest.raises(
            KeyError, match="Could not find column time_diff in DataFrame"
        ):
            self.dataframe_wrangler.calculate_weight(
                df_without_time_diff, decay_constant, base_value
            )
