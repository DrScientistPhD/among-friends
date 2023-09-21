from typing import Any, Dict, List, Tuple

import networkx as nx
import pandas as pd

from src.data.data_validation import validate_data_types, validate_dataframe
from src.data.sna_preparation import NodesEdgesDataProcessor
from src.models.sna_graph_builder import SnaGraphBuilder, SnaMetricCalculator


class NetworkGraphGenerator:
    def __init__(
        self,
        nodes_edges_df: pd.DataFrame,
        pos: Dict[str, Tuple[float, float]] = None,
    ) -> None:
        """
        Initializes the NetworkGraphGenerator class.

        Args:
            nodes_edges_df (pd.DataFrame): DataFrame containing nodes and edges information.
            pos (Dict[str, Tuple[float, float]], optional): Pre-defined graph layout positions. Defaults to None.

        Raises:
            TypeError: If nodes_edges_df is not a pandas DataFrame.
            Exception: If there's an error while generating graph layout using NetworkX.
        """
        validate_dataframe(nodes_edges_df)

        self.nodes_edges_df = nodes_edges_df
        try:
            if not pos:
                G = SnaGraphBuilder.create_network_graph(self.nodes_edges_df)
                self.pos = nx.kamada_kawai_layout(G)
            else:
                self.pos = pos
        except nx.NetworkXError as e:
            raise nx.NetworkXError(f"NetworkX error while generating graph layout: {e}")

    def generate_filtered_graph(
        self, date_range: Tuple[Any, Any]
    ) -> Tuple[nx.Graph, List[Any], Dict[str, Tuple[float, float]], Dict[str, Any]]:
        """
        Generates a filtered graph using the provided date range.

        Args:
            date_range (Tuple[Any, Any]): Tuple containing the start and end dates for filtering.

        Returns:
            Tuple[nx.Graph, List[Any], Dict[str, Tuple[float, float]], Dict[str, Any]]:
                - nx.Graph: The filtered graph.
                - List[Any]: List of participants in the graph.
                - Dict[str, Tuple[float, float]]: Graph layout positions.
                - Dict[str, Any]: Eigenvector metrics of the graph.

        Raises:
            TypeError: If date_range is not a tuple.
            Exception: If there is an error while generating the filtered graph.
        """
        validate_data_types(date_range, Tuple, "date_range")

        start_date, end_date = date_range
        try:
            nodes_edges_filtered_df = NodesEdgesDataProcessor.filter_dataframe_by_dates(
                self.nodes_edges_df, start_date, end_date
            )

            G = SnaGraphBuilder.create_network_graph(nodes_edges_filtered_df)
            participants = list(G.nodes)
            eigenvector_metrics = SnaMetricCalculator.generate_eigenvector_metrics(G)
        except Exception as e:
            raise Exception(f"Error encountered while generating filtered graph {e}")
        return G, participants, self.pos, eigenvector_metrics
