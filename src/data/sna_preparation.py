import warnings
from typing import Dict, List, Optional

import networkx as nx
import pandas as pd

from src.data.data_validation import (validate_columns_in_dataframe,
                                      validate_data_types, validate_dataframe)
from src.data.data_wrangling import (DateTimeConverter, EmojiDataWrangler,
                                     MessageDataWrangler,
                                     QuotationResponseDataWrangler)
from src.data.time_calculations import TimeCalculations


class SnaDataWrangler:
    @staticmethod
    def standardize_response_react_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a standardized dataframe for comment-response pairs by selecting only the relevant columns and adding
        a reaction category.

        Args:
            df (df.DataFrame): The input comment-response DataFrame.

        Returns:
            pd.DataFrame: The standardized comment-response DataFrame.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)

        try:
            df = df.rename(
                columns={
                    "comment_from_recipient_id": "target_participant_id",
                    "comment_date_sent_datetime": "target_datetime",
                    "response_from_recipient_id": "source_participant_id",
                    "response_date_sent_datetime": "source_datetime",
                }
            )

            columns_to_keep = [
                "target_participant_id",
                "target_datetime",
                "source_participant_id",
                "source_datetime",
                "weight",
            ]

            standardized_df = df[columns_to_keep].copy()

            standardized_df["interaction_category"] = "response"
            return standardized_df

        except Exception as e:
            raise Exception(
                f"Failed to subset and assign new category to input dataframe."
            )

    @staticmethod
    def standardize_emoji_react_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a standardized dataframe for comment-emoji pairs by selecting only the relevant columns and adding
        a reaction category.

        Args:
            df (df.DataFrame): The input comment-emoji DataFrame.

        Returns:
            pd.DataFrame: The standardized comment-emoji DataFrame.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)

        try:
            df = df.rename(
                columns={
                    "comment_from_recipient_id": "target_participant_id",
                    "comment_date_sent_datetime": "target_datetime",
                    "emoji_author_id": "source_participant_id",
                    "emoji_date_sent_datetime": "source_datetime",
                }
            )

            columns_to_keep = [
                "target_participant_id",
                "target_datetime",
                "source_participant_id",
                "source_datetime",
                "weight",
            ]

            standardized_df = df[columns_to_keep].copy()

            standardized_df["interaction_category"] = "emoji"
            return standardized_df

        except Exception as e:
            raise Exception(
                f"Failed to subset and assign new category to input dataframe."
            )

    @staticmethod
    def standardize_quotation_react_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a standardized dataframe for quotation-response pairs by selecting only the relevant columns and adding
        a reaction category.

        Args:
            df (df.DataFrame): The input quotation-response DataFrame.

        Returns:
            pd.DataFrame: The standardized quotation-response DataFrame.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            Exception: If the standardization process fails.
        """
        # Validate input data
        validate_dataframe(df)

        try:
            df = df.rename(
                columns={
                    "quotation_from_recipient_id": "target_participant_id",
                    "quotation_date_sent_datetime": "target_datetime",
                    "response_from_recipient_id": "source_participant_id",
                    "response_date_sent_datetime": "source_datetime",
                }
            )

            columns_to_keep = [
                "target_participant_id",
                "target_datetime",
                "source_participant_id",
                "source_datetime",
                "weight",
            ]

            standardized_df = df[columns_to_keep].copy()

            standardized_df["interaction_category"] = "quotation"
            return standardized_df

        except Exception as e:
            raise Exception(
                f"Failed to subset and assign new category to input dataframe."
            )

    @staticmethod
    def process_data_for_sna(
        interaction_type: str,
        base_value: float,
        message_slim_df: pd.DataFrame,
        emoji_slim_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Process data based on the data type.
        Never make a function like this again. Next time, plan out column names better in advance.

        Args:
            interaction_type (str): The type of interaction to be processed. One of "message", "emoji", "quotation".
            base_value (float): Base value used to derive the weight of interactions.
            message_slim_df (DataFrame): A preprocessed message dataframe.
            emoji_slim_df (Optional[DataFrame]): A preprocessed emoji dataframe. Required only if interaction_type is "emoji".

        Returns:
            DataFrame: The processed dataframe.

        Raises:
            TypeError: If the interaction_type is not a str, or the base_value is not a float, or the dataframes are not valid.
            ValueError: If the provided `data_type` is not one of "response", "emoji", "quotation".
            Exception: For other exceptions during processing.
        """
        # Validate input data types
        validate_data_types(interaction_type, str, "interaction_type")
        validate_data_types(base_value, float, "base_value")
        validate_dataframe(message_slim_df)

        if interaction_type == "emoji":
            validate_dataframe(emoji_slim_df)

        try:
            if interaction_type == "response":
                group_n = message_slim_df["comment_from_recipient_id"].nunique()
                processed_df = MessageDataWrangler.concatenate_comment_threads(
                    message_slim_df, group_n
                )
            elif interaction_type == "emoji":
                processed_df = EmojiDataWrangler.merge_message_with_emoji(
                    message_slim_df, emoji_slim_df
                )
            elif interaction_type == "quotation":
                processed_df = (
                    QuotationResponseDataWrangler.create_quotation_response_df(
                        message_slim_df
                    )
                )
            else:
                raise ValueError(
                    f"Invalid data_type: {interaction_type}. Expected one of ['response', 'emoji', 'quotation']."
                )

            # Calculate time differences, decay constants, and weights
            if interaction_type == "quotation":
                time_col_1 = "response_date_sent"
                time_col_2 = "quotation_date_sent"
            else:
                time_col_1 = interaction_type + "_date_sent"
                time_col_2 = "comment_date_sent"

            processed_df = TimeCalculations.calculate_time_diff(
                processed_df, time_col_1, time_col_2
            )
            decay_constant = TimeCalculations.calculate_decay_constant(
                processed_df, "time_diff"
            )
            processed_df = TimeCalculations.calculate_weight(
                processed_df, decay_constant, base_value
            )

            # Convert Unix timestamps to readable datetime formats
            processed_df = DateTimeConverter.convert_unix_to_datetime(
                processed_df, time_col_2
            )
            processed_df = DateTimeConverter.convert_unix_to_datetime(
                processed_df, time_col_1
            )

            # Standardize dataframe
            if interaction_type == "response":
                return SnaDataWrangler.standardize_response_react_dataframe(
                    processed_df
                )
            elif interaction_type == "emoji":
                return SnaDataWrangler.standardize_emoji_react_dataframe(processed_df)
            elif interaction_type == "quotation":
                return SnaDataWrangler.standardize_quotation_react_dataframe(
                    processed_df
                )

        except Exception as e:
            raise Exception(
                f"An error occurred while processing {interaction_type} data: {e}"
            )

    @staticmethod
    def concatenate_dataframes_vertically(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Concatenate a list of pandas DataFrames vertically after ensuring they all have the same columns.

        Args:
            dfs (List[pd.DataFrame]): List of pandas DataFrames to concatenate.

        Returns:
            pd.DataFrame: The concatenated DataFrame.

        Raises:
            ValueError: If not all DataFrames have the same column names.
        """
        # Validate input data types
        validate_data_types(dfs, List, "dfs")

        try:
            # Check if all dataframes have the same columns
            columns_set = set(dfs[0].columns)
            for df in dfs[1:]:
                if set(df.columns) != columns_set:
                    raise ValueError("Not all DataFrames have the same column names.")

            # Concatenate dataframes vertically
            concatenated_df = pd.concat(dfs, axis=0, ignore_index=True)

            return concatenated_df

        except Exception as e:
            raise Exception(
                f"An error occurred while processing concatenating dataframes data: {e}"
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
    def generate_eigenvector_closeness_metrics(graph: nx.DiGraph) -> Dict:
        """
        Generate a hierarchical data structure containing eigenvector centrality rank, eigenvector centrality score,
        and closeness rankings for each node.

        Args:
            graph (nx.DiGraph): A directed graph.

        Returns:
            A hierarchical data structure.

        Raises:
            TypeError: If the input graph is not of type nx.DiGraph.
            Warning: If eigenvector centrality computation does not converge after two attempts.
        """
        # Validate input data types
        validate_data_types(graph, nx.DiGraph, "graph")

        # Attempt to compute eigenvector centrality
        try:
            eigenvector_centrality = nx.eigenvector_centrality(graph, weight='weight')
        except nx.PowerIterationFailedConvergence:
            warnings.warn("Eigenvector centrality failed to converge using default iterations. Setting default values.")
            eigenvector_centrality = {node: 0 for node in graph.nodes()}  # Default to 0

        eigenvector_ranking = {node: rank for rank, node in
                               enumerate(sorted(eigenvector_centrality, key=eigenvector_centrality.get, reverse=True),
                                         1)}

        # Initialize the hierarchical data structure
        hierarchy = {}

        for node in graph.nodes():
            hierarchy[node] = {
                "Eigenvector Rank": eigenvector_ranking[node],
                "Eigenvector Score": eigenvector_centrality[node],
                "Closeness Ranking": {}
            }

            # Compute closeness rankings for each node
            path_lengths = nx.single_source_dijkstra_path_length(graph, node)
            # Filter out the current node itself from the closeness rankings
            filtered_path_lengths = {k: v for k, v in path_lengths.items() if k != node}
            # Sort the nodes in descending order (i.e., closest nodes first)
            sorted_nodes = sorted(filtered_path_lengths.items(), key=lambda x: x[1])
            hierarchy[node]["Closeness Ranking"] = {node: distance for node, distance in sorted_nodes}

        return hierarchy
