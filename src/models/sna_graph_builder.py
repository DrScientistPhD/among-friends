import warnings
from typing import Dict, Optional

import networkx as nx
import pandas as pd

from src.data.data_validation import (
    validate_columns_in_dataframe,
    validate_data_types,
    validate_dataframe,
)


class SnaGraphBuilder:
    @staticmethod
    def create_network_graph(
        df: pd.DataFrame, interaction_category: Optional[str] = None
    ) -> nx.DiGraph:
        """
        Create a directed graph using networkx based on the interaction data in the nodes_edges dataframe.

        Args:
            df (pd.DataFrame): The input dataframe containing nodes and edges data.
            interaction_category (Optional[str]): The interaction category to filter by. Valid options:
                "response", "emoji", "quotation". If None, all interaction categories are included.

        Returns:
            A directed graph.

        Raises:
            TypeError: If df is not a pandas DataFrame, or interaction_category is not a string or NoneType.
            ValueError: If the provided `data_type` is not one of "response", "emoji", "quotation", or if the required
                columns are not in df.
            Exception: For other exceptions during processing.
        """
        # Validate input data types
        validate_dataframe(df)
        validate_data_types(
            interaction_category, (str, type(None)), "interaction_category"
        )
        validate_columns_in_dataframe(
            df,
            [
                "target_participant_id",
                "target_datetime",
                "source_participant_id",
                "source_datetime",
                "weight",
                "interaction_category",
            ],
        )

        # Filter by interaction category if specified
        if interaction_category:
            valid_categories = ["response", "emoji", "quotation"]
            if interaction_category not in valid_categories:
                raise ValueError(
                    f"Invalid interaction_category. Expected one of {valid_categories}, but got {interaction_category}."
                )

            df = df[df["interaction_category"] == interaction_category]

            # Check if the filtered dataframe is empty
            if df.empty:
                raise ValueError(
                    f"No data found for the interaction category: {interaction_category}"
                )

        try:
            # Initialize a directed graph
            G = nx.DiGraph()

            # Add nodes
            for participant_id in pd.concat(
                [df["source_participant_id"], df["target_participant_id"]]
            ).unique():
                G.add_node(participant_id)

            # Add edges with attributes
            for _, row in df.iterrows():
                G.add_edge(
                    row["source_participant_id"],
                    row["target_participant_id"],
                    weight=row["weight"],
                    interaction_category=row["interaction_category"],
                    source_datetime=row["source_datetime"],
                    target_datetime=row["target_datetime"],
                )

            return G

        except Exception as e:
            raise Exception(f"An error occurred while creating the network graph: {e}")


class SnaMetricCalculator:
    @staticmethod
    def generate_eigenvector_metrics(graph: nx.DiGraph) -> Dict[str, dict]:
        """
        Generate eigenvector centrality rank and score for each node.

        Args:
            graph (nx.DiGraph): A directed graph.

        Returns:
            A dictionary containing eigenvector rank and score for each node.

        Raises:
            TypeError: If the input graph is not of type nx.DiGraph.
            Warning: If eigenvector centrality computation does not converge after two attempts.
        """
        # Validate input data types
        validate_data_types(graph, nx.DiGraph, "graph")

        # Attempt to compute eigenvector centrality
        try:
            eigenvector_centrality = nx.eigenvector_centrality(graph, weight="weight")
        except nx.PowerIterationFailedConvergence:
            warnings.warn(
                "Eigenvector centrality failed to converge using default iterations. Setting default values."
            )
            eigenvector_centrality = {node: 0 for node in graph.nodes()}  # Default to 0

        eigenvector_ranking = {
            node: rank
            for rank, node in enumerate(
                sorted(
                    eigenvector_centrality, key=eigenvector_centrality.get, reverse=True
                ),
                1,
            )
        }

        return {
            "Eigenvector Rank": eigenvector_ranking,
            "Eigenvector Score": eigenvector_centrality
        }

    @staticmethod
    def generate_closeness_metrics(graph: nx.DiGraph) -> Dict:
        """
        Generate closeness rankings for each node.

        Args:
            graph (nx.DiGraph): A directed graph.

        Returns:
            A dictionary containing closeness rankings for each node.

        Raises:
            TypeError: If the input graph is not of type nx.DiGraph.
        """

        closeness_ranking = {}

        for node in graph.nodes():
            # Compute closeness rankings for each node
            path_lengths = nx.single_source_dijkstra_path_length(graph, node)
            # Filter out the current node itself from the closeness rankings
            filtered_path_lengths = {k: v for k, v in path_lengths.items() if k != node}
            # Sort the nodes in descending order (i.e., closest nodes first)
            sorted_nodes = sorted(filtered_path_lengths.items(), key=lambda x: x[1])
            closeness_ranking[node] = {
                node: distance for node, distance in sorted_nodes
            }

        return {"Closeness Ranking": closeness_ranking}
