from typing import Any, Dict, List, Tuple

import networkx as nx
import pandas as pd

from src.data.sna_preparation import NodesEdgesDataProcessor
from src.models.sna_graph_builder import SnaGraphBuilder, SnaMetricCalculator


class NetworkGraphGenerator:
    def __init__(
        self,
        nodes_edges_df: pd.DataFrame,
        pos: Dict[str, Tuple[float, float]] = None,
    ) -> None:
        self.nodes_edges_df = nodes_edges_df
        # Generate a graph layout just once if not provided
        if not pos:
            G = SnaGraphBuilder.create_network_graph(self.nodes_edges_df)
            self.pos = nx.spring_layout(G)
        else:
            self.pos = pos

    def generate_filtered_graph(
        self, date_range: Tuple[Any, Any]
    ) -> Tuple[nx.Graph, List[Any], Dict[str, Tuple[float, float]], Dict[str, Any]]:
        start_date, end_date = date_range
        nodes_edges_filtered_df = NodesEdgesDataProcessor.filter_dataframe_by_dates(
            self.nodes_edges_df, start_date, end_date
        )
        G = SnaGraphBuilder.create_network_graph(nodes_edges_filtered_df)
        participants = list(G.nodes)
        eigenvector_metrics = SnaMetricCalculator.generate_eigenvector_metrics(G)
        return G, participants, self.pos, eigenvector_metrics  # Use the stored layout
