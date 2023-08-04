import numpy as np
import pandas as pd
import pytest
from faker import Faker

from src.data.time_calculations import TimeCalculations


class TestTimeCalculations:
    """
    Test class for the TimeCalculations class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.time_calculations = TimeCalculations()

    @pytest.mark.parametrize("iteration", range(10))
    def test_calculate_time_diff(self, iteration, fake_quotation_response_df):
        """
        Test to verify that the function `calculate_time_diff` correctly calculates the time difference
        between quotation and response date_sent and adds it as a new column to the DataFrame.
        """
        # Use the calculate_time_diff() function to calculate the time difference
        time_diff_df = self.time_calculations.calculate_time_diff(
            fake_quotation_response_df, "quotation_date_sent", "response_date_sent"
        )

        # Check that the output is a DataFrame
        assert isinstance(time_diff_df, pd.DataFrame)

        # Check that the 'time_diff' column exists in the output DataFrame
        assert "time_diff" in time_diff_df.columns

        # Check that a warning is raised when time_diff is negative
        with pytest.warns(UserWarning):
            self.time_calculations.calculate_time_diff(
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
        with pytest.raises(TypeError):
            self.time_calculations.calculate_time_diff(
                "invalid_data", "quotation_date_sent", "response_date_sent"
            )

        # Try calling calculate_time_diff() with DataFrame but non-existing column names
        with pytest.raises(KeyError):
            self.time_calculations.calculate_time_diff(
                fake_quotation_response_df,
                "non_existent_start_time",
                "non_existent_end_time",
            )

        # Check if the function correctly calculates time difference even when timestamps are equal
        fake_quotation_response_df_equal_timestamps = fake_quotation_response_df.copy()
        fake_quotation_response_df_equal_timestamps[
            "quotation_date_sent"
        ] = fake_quotation_response_df_equal_timestamps["response_date_sent"]

        result_df = self.time_calculations.calculate_time_diff(
            fake_quotation_response_df_equal_timestamps,
            "quotation_date_sent",
            "response_date_sent",
        )

        assert (
            result_df["time_diff"] == 0
        ).all(), "Time difference not correctly calculated for equal timestamps."

    @pytest.mark.parametrize("percentile", [0.0, 0.5, 0.75, 0.99, 1.0])
    def test_calculate_decay_constant(self, percentile, fake_quotation_response_df):
        """
        Test to verify that the function `calculate_decay_constant` correctly calculates the decay constant based
        on the specified percentile of a specified column in the given DataFrame.
        """
        # Use the calculate_decay_constant() function to calculate the decay constant
        decay_constant = self.time_calculations.calculate_decay_constant(
            fake_quotation_response_df, "time_diff", percentile
        )

        # Check that the output is a float
        assert isinstance(decay_constant, float)

        # Try calling calculate_decay_constant() with DataFrame but non-existing column names
        with pytest.raises(KeyError):
            self.time_calculations.calculate_decay_constant(
                fake_quotation_response_df, "non_existent_column", percentile
            )

        # Try calling calculate_decay_constant() with invalid percentile
        with pytest.raises(ValueError):
            self.time_calculations.calculate_decay_constant(
                fake_quotation_response_df, "time_diff", -1
            )

        # Check the function with default percentile
        default_decay_constant = self.time_calculations.calculate_decay_constant(
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
        df_with_weight = self.time_calculations.calculate_weight(
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
        with pytest.raises(TypeError):
            self.time_calculations.calculate_weight(
                "not_a_dataframe", decay_constant, base_value
            )
        with pytest.raises(TypeError):
            self.time_calculations.calculate_weight(
                fake_quotation_response_df, "not_a_float", base_value
            )
        # Uncommented as you requested
        with pytest.raises(TypeError):
            self.time_calculations.calculate_weight(
                fake_quotation_response_df, decay_constant, "not_a_float"
            )

        # Try calling calculate_weight() with a DataFrame without 'time_diff' column
        df_without_time_diff = fake_quotation_response_df.drop(columns=["time_diff"])
        with pytest.raises(KeyError):
            self.time_calculations.calculate_weight(
                df_without_time_diff, decay_constant, base_value
            )
