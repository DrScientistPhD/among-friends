from typing import Any, List, Tuple

import holoviews as hv
import pandas as pd
import panel as pn

from src.data.sna_preparation import NodesEdgesDataProcessor
from src.visualization.generative_text_page import GenerativeText
from src.visualization.graph_generators import NetworkGraphGenerator
from src.visualization.social_network_page import SocialNetworkPage
from src.visualization.statistics_page import StatisticsPage
from src.visualization.ui_components import NetworkUIComponents

# Constants and Colors
ACCENT = "#BB2649"


class AppManager:
    def __init__(self) -> None:
        # Ensure the holoviews extension is loaded at the start of the app initialization
        hv.extension("bokeh")

        self.data_processor = NodesEdgesDataProcessor()
        self.nodes_edges_df = self.data_processor.fake_nodes_edges_dataframe()
        self.min_date = self.nodes_edges_df["target_datetime"].apply(pd.Timestamp).min()
        self.max_date = self.nodes_edges_df["target_datetime"].apply(pd.Timestamp).max()

        self.init_network_ui_components()

        self.main_content = pn.Row(
            SocialNetworkPage(
                self.date_range_slider,
                self.node_selector,
                self.min_date,
                self.max_date,
                self.ui_components,
            ).view(),
            sizing_mode="stretch_both",
        )
        self.template = self.setup_template()

    def init_network_ui_components(self) -> None:
        self.date_range_slider = self.create_network_date_range_slider()
        self.graph_generator = self.create_network_graph_generator()
        self.node_selector = self.create_network_node_selector()
        self.ui_components = NetworkUIComponents(
            self.data_processor,
            self.graph_generator,
            self.date_range_slider,
            self.node_selector,
        )

    def create_network_date_range_slider(self) -> pn.widgets.DateRangeSlider:
        return pn.widgets.DateRangeSlider(
            name="Group Chat Date Range",
            start=self.min_date,
            end=self.max_date,
            value=(self.min_date, self.max_date),
            show_value=False,
            bar_color="red",
        )

    def create_network_graph_generator(self) -> NetworkGraphGenerator:
        return NetworkGraphGenerator(self.nodes_edges_df)

    def create_network_node_selector(self) -> pn.widgets.Select:
        participants = sorted(
            list(
                self.graph_generator.generate_filtered_graph(
                    (self.min_date, self.max_date)
                )[1]
            )
        )
        return pn.widgets.Select(options=participants, name="Group Chat Participant")

    def setup_template(self) -> pn.template.FastListTemplate:
        template = pn.template.FastListTemplate(
            title="Among Friends", accent_base_color=ACCENT, header_background=ACCENT
        )

        # Setup buttons and event handlers
        self.setup_template_buttons(template)

        # Set the initial content
        template.main.append(self.main_content)
        return template

    def setup_template_buttons(self, template: pn.template.FastListTemplate) -> None:
        buttons: List[Tuple[str, callable]] = [
            ("Network Analysis", self.load_network_analysis_page),
            ("Statistics", self.load_statistics_page),
            ("Generative Text", self.load_gen_text_page),
        ]

        for btn_name, btn_event in buttons:
            btn = pn.widgets.Button(name=btn_name, width_policy="max")
            btn.on_click(btn_event)
            template.sidebar.append(btn)

    def load_network_analysis_page(self, event: Any) -> None:
        self.main_content.clear()
        self.main_content.append(
            SocialNetworkPage(
                self.date_range_slider,
                self.node_selector,
                self.min_date,
                self.max_date,
                self.ui_components,
            ).view()
        )

    def load_statistics_page(self, event: Any) -> None:
        self.main_content.clear()
        self.main_content.append(StatisticsPage().view())

    def load_gen_text_page(self, event: Any) -> None:
        self.main_content.clear()
        self.main_content.append(GenerativeText().view())

    def serve(self) -> None:
        pn.serve(self.template)


app_manager = AppManager()
app_manager.serve()
