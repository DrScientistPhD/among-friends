import networkx as nx
import numpy as np

from src.visualization.graph_generators import NetworkGraphGenerator


class TestNetworkGraphGenerator:
    def test_initialization_with_default_pos(self, fake_nodes_edges_dataframe):
        """Test initialization with default position."""
        graph_generator = NetworkGraphGenerator(fake_nodes_edges_dataframe)
        assert isinstance(graph_generator, NetworkGraphGenerator)
        assert "pos" in dir(graph_generator)

    def test_initialization_with_custom_pos(self, fake_nodes_edges_dataframe):
        """Test initialization with custom position."""
        pos = {(i, i): (i * 10, i * 20) for i in range(10)}
        graph_generator = NetworkGraphGenerator(fake_nodes_edges_dataframe, pos)
        assert isinstance(graph_generator, NetworkGraphGenerator)
        assert graph_generator.pos == pos

    def test_generate_filtered_graph(self, fake_nodes_edges_dataframe):
        """Test graph generation with filtering."""
        graph_generator = NetworkGraphGenerator(fake_nodes_edges_dataframe)
        date_range = ("2022-01-01", "2023-01-01")
        (
            graph,
            participants,
            positions,
            node_sizes,
        ) = graph_generator.generate_filtered_graph(date_range)

        assert isinstance(graph, nx.Graph)

        # Ensure all participants are integers or numpy.int64
        assert all(
            isinstance(participant, (int, np.int64)) for participant in participants
        )

    def test_generate_filtered_graph_structure(self, fake_nodes_edges_dataframe):
        """Test the structure of the generated graph."""
        graph_generator = NetworkGraphGenerator(fake_nodes_edges_dataframe)
        date_range = ("2022-01-01", "2023-01-01")
        graph_data = graph_generator.generate_filtered_graph(date_range)

        # Ensure the method returns a tuple
        assert isinstance(graph_data, tuple)

        # Extract data from the tuple
        graph, participants, positions, node_sizes = graph_data

        assert isinstance(graph, nx.Graph)

        # Ensure all participants are integers or numpy.int64
        assert all(
            isinstance(participant, (int, np.int64)) for participant in participants
        )

        # Ensure the positions dictionary has the correct format
        problematic_items = [
            (key, value)
            for key, value in positions.items()
            if not isinstance(key, (int, np.int64))
            or not (isinstance(value, tuple) and len(value) == 2)
            and not (isinstance(value, np.ndarray) and value.shape == (2,))
        ]
        assert (
            not problematic_items
        ), f"Problematic keys and values in positions: {problematic_items}"
