import os
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker

from src.gen_ai.agent_tools import AgentTools


class TestAgentTools:
    """
    Test class for the AgentTools class.
    """

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.fake = Faker()
        self.llm = MagicMock()  # Mock the ChatOpenAI object

        # Mock environment variables
        with patch.dict(
            os.environ,
            {"PINECONE_API_KEY": self.fake.pystr(), "PINECONE_ENV": self.fake.pystr()},
        ):
            self.agent_tools = AgentTools(self.llm)

    @pytest.mark.parametrize("iteration", range(10))
    def test_get_search_filter(self, iteration):
        """Test to check if the get_search_filter() function properly creates a search filter dictionary."""
        # Generate fake data
        sent_day_operator = self.fake.random_element(
            elements=("eq", "ne", "lt", "lte", "gt", "gte", "in", "nin")
        )
        sent_day = [self.fake.random_int(min=1, max=31) for _ in range(10)]
        sent_day_of_week_operator = self.fake.random_element(
            elements=("eq", "ne", "lt", "lte", "gt", "gte", "in", "nin")
        )
        sent_day_of_week = [self.fake.day_of_week() for _ in range(10)]
        sent_month_operator = self.fake.random_element(
            elements=("eq", "ne", "lt", "lte", "gt", "gte", "in", "nin")
        )
        sent_month = [self.fake.month_name() for _ in range(10)]
        sent_year_operator = self.fake.random_element(
            elements=("eq", "ne", "lt", "lte", "gt", "gte", "in", "nin")
        )
        sent_year = [self.fake.year() for _ in range(10)]

        # Use the get_search_filter function
        result = self.agent_tools.get_search_filter(
            sent_day_operator,
            sent_day,
            sent_day_of_week_operator,
            sent_day_of_week,
            sent_month_operator,
            sent_month,
            sent_year_operator,
            sent_year,
        )

        # Check if the result is a dictionary
        assert isinstance(result, dict)

        # Check if the result contains the correct keys
        assert "filter" in result
        assert all(
            key in result["filter"]
            for key in ("sent_day", "sent_day_of_week", "sent_month", "sent_year")
        )

        # Check if the result contains the correct values
        assert result["filter"]["sent_day"] == {sent_day_operator: sent_day}
        assert result["filter"]["sent_day_of_week"] == {
            sent_day_of_week_operator: sent_day_of_week
        }
        assert result["filter"]["sent_month"] == {sent_month_operator: sent_month}
        assert result["filter"]["sent_year"] == {sent_year_operator: sent_year}
