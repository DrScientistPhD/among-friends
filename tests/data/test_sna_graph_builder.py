import pytest
import networkx as nx

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
    def test_no_data_for_valid_interaction_category(self, iteration, fake_nodes_edges_dataframe):
        """
        Test the create_network_graph_modified function when a valid interaction category has no associated data.
        """
        # Modify the dataframe so it doesn't contain any rows with interaction_category = "response"
        df_no_response = fake_nodes_edges_dataframe[fake_nodes_edges_dataframe["interaction_category"] != "response"]
        with pytest.raises(ValueError):
            self.sna_graph_builder.create_network_graph(df_no_response, interaction_category="response")


class TestSnaMetricCalculator:
    """Test class for the SnaMetricCalculator class."""

    @pytest.fixture(autouse=True)
    def setup_class(self):
        """Fixture to set up resources before each test method."""
        self.sna_metric_calculator = SnaMetricCalculator()

    @pytest.mark.parametrize("iteration", range(10))
    def test_eigenvector_centrality_computation(self, iteration, fake_network_graph):
        """ Test the generate_eigenvector_closeness_metrics method to ensure it correctly computes
        eigenvector centrality scores for a given graph."""
        result = self.sna_metric_calculator.generate_eigenvector_closeness_metrics(fake_network_graph)

        # Try computing eigenvector centrality, handle if it does not converge
        try:
            eigenvector_centrality = nx.eigenvector_centrality(fake_network_graph, weight='weight')
        except nx.PowerIterationFailedConvergence:
            eigenvector_centrality = {node: 0 for node in fake_network_graph.nodes()}  # Default to 0

        for node in result:
            assert abs(result[node]['Eigenvector Score'] - eigenvector_centrality[node]) < 1e-6

    @pytest.mark.parametrize("iteration", range(10))
    def test_closeness_rankings(self, iteration, fake_network_graph):
        """Test the generate_eigenvector_closeness_metrics method to ensure it correctly computes
        closeness rankings for each node in a given graph."""
        result = self.sna_metric_calculator.generate_eigenvector_closeness_metrics(fake_network_graph)
        for node in result:
            path_lengths = nx.single_source_dijkstra_path_length(fake_network_graph, node)
            # Exclude the node itself
            path_lengths = {k: v for k, v in path_lengths.items() if k != node}
            sorted_nodes = sorted(path_lengths.items(), key=lambda x: x[1])
            assert list(result[node]['Closeness Ranking'].keys()) == [x[0] for x in sorted_nodes]