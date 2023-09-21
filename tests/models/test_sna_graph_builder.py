import networkx as nx
import pytest

from src.models.sna_graph_builder import SnaGraphBuilder, SnaMetricCalculator


class TestSnaGraphBuilder:
    """Test class for the SnaGraphBuilder class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.sna_graph_builder = SnaGraphBuilder()

    @pytest.mark.parametrize("iteration", range(10))
    def test_valid_input(self, iteration, fake_nodes_edges_dataframe):
        """
        Test the create_network_graph_modified function with valid input.
        """
        result = self.sna_graph_builder.create_network_graph(fake_nodes_edges_dataframe)
        assert isinstance(result, nx.DiGraph)

    @pytest.mark.parametrize("iteration", range(10))
    def test_no_data_for_valid_interaction_category(
        self, iteration, fake_nodes_edges_dataframe
    ):
        """
        Test the create_network_graph_modified function when a valid interaction category has no associated data.
        """
        # Modify the dataframe so it doesn't contain any rows with interaction_category = "response"
        df_no_response = fake_nodes_edges_dataframe[
            fake_nodes_edges_dataframe["interaction_category"] != "response"
        ]
        with pytest.raises(ValueError):
            self.sna_graph_builder.create_network_graph(
                df_no_response, interaction_category="response"
            )


class TestSnaMetricCalculator:
    """Test class for the SnaMetricCalculator class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.sna_metric_calculator = SnaMetricCalculator()

    @pytest.mark.parametrize("iteration", range(10))
    def test_generate_eigenvector_metrics_valid(self, iteration, fake_network_graph):
        """Test the generate_eigenvector_metrics method with valid input."""
        metrics = self.sna_metric_calculator.generate_eigenvector_metrics(
            fake_network_graph
        )
        assert isinstance(metrics, dict)
        assert "Eigenvector Rank" in metrics and "Eigenvector Score" in metrics

    @pytest.mark.parametrize("iteration", range(10))
    def test_generate_eigenvector_metrics_invalid_type(self, iteration):
        """Test the generate_eigenvector_metrics method with invalid input type."""
        with pytest.raises(TypeError):
            self.sna_metric_calculator.generate_eigenvector_metrics("invalid_type")

    @pytest.mark.parametrize("iteration", range(10))
    def test_generate_outward_response_metrics_valid(
        self, iteration, fake_network_graph
    ):
        """Test the generate_outward_response_metrics method with valid input."""
        metrics = self.sna_metric_calculator.generate_outward_response_metrics(
            fake_network_graph
        )
        assert isinstance(metrics, dict)
        assert (
            "Outward Response Rank" in metrics
            and "Outward Response Strength" in metrics
        )

    @pytest.mark.parametrize("iteration", range(10))
    def test_generate_outward_response_metrics_invalid_type(self, iteration):
        """Test the generate_outward_response_metrics method with invalid input type."""
        with pytest.raises(TypeError):
            self.sna_metric_calculator.generate_outward_response_metrics("invalid_type")

    @pytest.mark.parametrize("iteration", range(10))
    def test_generate_outward_response_metrics_insufficient_nodes(self, iteration):
        """Test the generate_outward_response_metrics method with a graph that lacks enough nodes."""
        G = nx.DiGraph()
        G.add_node("A")
        with pytest.raises(Exception):
            self.sna_metric_calculator.generate_outward_response_metrics(G)
