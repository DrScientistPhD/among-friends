import warnings
from typing import Any, Dict, Tuple, Union

import holoviews as hv
import pandas as pd
import panel as pn
from bokeh.models import HoverTool
from holoviews import opts

from src.models.sna_graph_builder import SnaMetricCalculator

# Constants and Colors
ACCENT = "#BB2649"


class NetworkUIComponents:
    def __init__(
        self,
        data_processor: Any,
        graph_generator: Any,
        date_range_slider: pn.widgets.DateRangeSlider,
        node_selector: pn.widgets.Select,
    ) -> None:
        self.start_date_pane = pn.pane.HTML(width=200)
        self.end_date_pane = pn.pane.HTML(width=200)
        self.data_processor = data_processor
        self.graph_generator = graph_generator
        self.date_range_slider = date_range_slider
        self.node_selector = node_selector

    @pn.depends("date_range_slider.value")
    def update_start_date_label(self) -> str:
        start_date, _ = self.date_range_slider.value
        return f"<font size='4'>Start Date: {start_date.strftime('%b %d, %Y')}</font>"

    @pn.depends("date_range_slider.value")
    def update_end_date_label(self) -> str:
        _, end_date = self.date_range_slider.value
        return f"<font size='4'>End Date: {end_date.strftime('%b %d, %Y')}</font>"

    @pn.depends("node_selector.value", "date_range_slider.value")
    def update_eigenvector_ranking_label(self) -> str:
        date_range = self.date_range_slider.value
        selected_node = self.node_selector.value
        _, _, _, eigenvector_metrics = self.graph_generator.generate_filtered_graph(
            date_range
        )
        rankings = eigenvector_metrics["Eigenvector Rank"]
        if selected_node in rankings:
            rank = rankings[selected_node]
            total_participants = len(rankings)
            return (
                f"<font size='4'>Influencer Ranking: {rank}/{total_participants}</font>"
            )
        else:
            return (
                "<font size='4'>Participant not found in the current date range</font>"
            )

    @staticmethod
    def set_node_colors_and_rankings(
        G: hv.Graph, node_selector_value: Any, eigenvector_metrics: Dict[str, Any]
    ) -> None:
        for node in G.nodes():
            G.nodes[node]["color"] = (
                "#ffa07a" if node == node_selector_value else "#add8e6"
            )
            rankings = eigenvector_metrics["Eigenvector Rank"]
            G.nodes[node]["influencer_ranking"] = rankings.get(node, "N/A")

    @staticmethod
    def adjust_edge_weights(G: hv.Graph) -> None:
        weights = [d["weight"] for _, _, d in G.edges(data=True)]
        max_weight = max(weights)
        min_weight = min(weights)
        if max_weight == min_weight:
            normalized_weights = [0.5 for _ in weights]
        else:
            normalized_weights = [
                (w - min_weight) / (max_weight - min_weight) for w in weights
            ]

        power = 2
        adjusted_weights = [nw**power for nw in normalized_weights]
        min_edge_thickness = 0.5
        max_edge_thickness = 5
        edge_widths = [
            min_edge_thickness + aw * (max_edge_thickness - min_edge_thickness)
            for aw in adjusted_weights
        ]

        for (_, _, d), width in zip(G.edges(data=True), edge_widths):
            d["edge_width"] = width

    @staticmethod
    def set_node_sizes(G: hv.Graph, eigenvector_metrics: Dict[str, Any]) -> None:
        eigenvector_scores = eigenvector_metrics["Eigenvector Score"]
        max_size, min_size = 50, 10
        min_eigenvector_score = min(eigenvector_scores.values())
        max_eigenvector_score = max(eigenvector_scores.values())
        denominator = max_eigenvector_score - min_eigenvector_score
        for node in G.nodes():
            if denominator == 0:
                G.nodes[node]["size"] = min_size
            else:
                size = min_size + (
                    eigenvector_scores[node] - min_eigenvector_score
                ) / denominator * (max_size - min_size)
                G.nodes[node]["size"] = size

    @pn.depends("node_selector.value", "date_range_slider.value")
    def get_network_graph(
        self, node_selector_value: Any = None, date_range_value: Any = None
    ) -> hv.Graph:
        if not node_selector_value or not date_range_value:
            return pn.pane.HTML("<i>Waiting for data...</i>")

        G, _, _, eigenvector_metrics = self.graph_generator.generate_filtered_graph(
            date_range_value
        )

        NetworkUIComponents.set_node_colors_and_rankings(
            G, node_selector_value, eigenvector_metrics
        )
        NetworkUIComponents.adjust_edge_weights(G)
        NetworkUIComponents.set_node_sizes(G, eigenvector_metrics)

        # Create a Bokeh HoverTool to customize hover information
        hover = HoverTool(
            tooltips=[
                ("Participant: ", "@index"),
                ("Influencer Ranking", "@influencer_ranking"),
            ]
        )
        graph = hv.Graph.from_networkx(G, self.graph_generator.pos).opts(
            opts.Graph(
                width=700,
                height=600,
                tools=[hover, "tap"],
                node_size="size",
                edge_line_width="edge_width",
                edge_alpha=0.5,
                node_color="color",
                edge_color=ACCENT,
                xaxis=None,
                yaxis=None,
            )
        )

        return graph

    def closeness_ranking_for_node(
        self,
        date_range: Tuple[Union[str, int, float], Union[str, int, float]],
        node: Any,
    ) -> pd.DataFrame:
        G, _, _, _ = self.graph_generator.generate_filtered_graph(date_range)
        closeness_metrics = SnaMetricCalculator.generate_closeness_metrics(G)
        rankings = closeness_metrics["Closeness Rank"].get(node, None)
        if rankings is None:
            warnings.warn(f"No closeness rankings found for node: {node}")
            return pd.DataFrame(columns=["Participant", "Closeness Ranking"])
        data = [
            {"Participant": participant, "Closeness Ranking": distance_rank}
            for participant, distance_rank in rankings.items()
        ]
        df = pd.DataFrame(data)
        if "Closeness Ranking" in df.columns:
            df = df.sort_values(by="Closeness Ranking")[
                ["Participant", "Closeness Ranking"]
            ]
        else:
            warnings.warn("No 'Closeness Ranking' column found in the DataFrame.")
            df = pd.DataFrame
        return df
