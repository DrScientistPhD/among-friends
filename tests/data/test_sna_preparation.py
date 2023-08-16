import pandas as pd
import pytest
from faker import Faker

from src.data.sna_preparation import SnaDataWrangler


class TestSnaDataWrangler:
    """
    Test class for the SnaDataWrangler class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """
        Fixture to set up resources before each test method.
        """
        self.fake = Faker()
        self.sna_data_wrangler = SnaDataWrangler()
        self.valid_columns = [
            "comment_from_recipient_id",
            "comment_date_sent_datetime",
            "response_from_recipient_id",
            "response_date_sent_datetime",
            "weight",
            "reaction_author_id",
            "reaction_date_sent_datetime",
            "quotation_from_recipient_id",
            "quotation_date_sent_datetime",
        ]

    @pytest.mark.parametrize("iteration", range(10))
    def test_create_reacted_dataframe_valid(self, iteration, fake_response_weight_df):
        """
        Test to check if create_reacted_dataframe() creates a new DataFrame by subsetting columns and adding a reaction category.
        """
        # Randomly select some of the valid columns
        random_cols = self.fake.random_elements(
            elements=self.valid_columns,
            length=self.fake.random_int(min=1, max=len(self.valid_columns)),
        )

        reaction_category = "test_category"

        new_data_frame = self.sna_data_wrangler.create_reacted_dataframe(
            fake_response_weight_df, random_cols, reaction_category
        )

        assert isinstance(new_data_frame, pd.DataFrame)
        assert list(new_data_frame.columns) == random_cols + ["interaction_category"]
        assert new_data_frame["interaction_category"].unique()[0] == reaction_category


    @pytest.mark.parametrize("iteration", range(10))
    def test_create_reacted_dataframe_invalid_input(self, iteration):
        """
        Test to check if the function raises appropriate exceptions with invalid input.
        """
        invalid_df = "invalid_data"  # Non-DataFrame input
        columns_to_keep = self.valid_columns
        interaction_category = "test_category"

        with pytest.raises(Exception):
            self.sna_data_wrangler.create_reacted_dataframe(invalid_df, columns_to_keep, interaction_category)

        invalid_columns_to_keep = "invalid_columns"  # Non-List input for columns_to_keep
        with pytest.raises(Exception):
            self.sna_data_wrangler.create_reacted_dataframe(pd.DataFrame(), invalid_columns_to_keep, interaction_category)

        invalid_interaction_category = 12345  # Non-string input for reaction_category
        with pytest.raises(Exception):
            self.sna_data_wrangler.create_reacted_dataframe(pd.DataFrame(), columns_to_keep, invalid_interaction_category)