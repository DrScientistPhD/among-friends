from typing import Any, List, Tuple
import click
import holoviews as hv
import pandas as pd
import panel as pn

from src.data.csv_mover import CSVMover
from src.data.sna_preparation import NodesEdgesDataProcessor
from src.visualization.generative_text_page import GenerativeText
from src.visualization.graph_generators import NetworkGraphGenerator
from src.visualization.social_network_page import SocialNetworkPage
from src.visualization.statistics_page import StatisticsPage
from src.visualization.ui_components import NetworkUIComponents

# Constants and Colors
ACCENT = "#BB2649"


class AppManager:
    """
    Main manager for the Among Friends application.
    Handles UI components initialization and page navigation.
    """
    def __init__(self, data_source: str = "production_data") -> None:
        """
        Initialize the AppManager and related UI components.

        Raises:
            Exception: If there's any error during initialization.
        """
        try:
            hv.extension("bokeh")

            csv_mover = CSVMover()
            self.nodes_edges_df = csv_mover.import_csv(f"data/{data_source}/processed", "nodes_edges_df")

            self.data_processor = NodesEdgesDataProcessor()
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
        except Exception as e:
            raise Exception(f"Error initializing network UI components: {e}")

    def init_network_ui_components(self) -> None:
        """
        Initialize the network UI components.
        """
        try:
            self.date_range_slider = self.create_network_date_range_slider()
            self.graph_generator = self.create_network_graph_generator()
            self.node_selector = self.create_network_node_selector()
            self.ui_components = NetworkUIComponents(
                self.data_processor,
                self.graph_generator,
                self.date_range_slider,
                self.node_selector,
            )
        except Exception as e:
            raise Exception(f"Error initializing network UI components: {e}")

    def create_network_date_range_slider(self) -> pn.widgets.DateRangeSlider:
        """
        Create and return the date range slider for network analysis.

        Returns:
            pn.widgets.DateRangeSlider: Configured date range slider widget.
        """
        try:
            return pn.widgets.DateRangeSlider(
                name="Group Chat Date Range",
                start=self.min_date,
                end=self.max_date,
                value=(self.min_date, self.max_date),
                show_value=False,
                bar_color="red",
            )
        except Exception as e:
            raise Exception(f"Error creating network date range slider: {e}")

    def create_network_graph_generator(self) -> NetworkGraphGenerator:
        """
        Create and return the network graph generator.

        Returns:
            NetworkGraphGenerator: Configured network graph generator.
        """
        try:
            return NetworkGraphGenerator(self.nodes_edges_df)
        except Exception as e:
            raise Exception(f"Error creating network graph generator: {e}")

    def create_network_node_selector(self) -> pn.widgets.Select:
        """
        Create and return the network node selector.

        Returns:
            pn.widgets.Select: Configured node selector widget.
        """
        try:
            participants = sorted(
                list(
                    self.graph_generator.generate_filtered_graph(
                        (self.min_date, self.max_date)
                    )[1]
                )
            )
            return pn.widgets.Select(options=participants, name="Group Chat Participant")
        except Exception as e:
            raise Exception(f"Error creating network node selector: {e}")

    def setup_template(self) -> pn.template.FastListTemplate:
        """
        Setup the main template for the application.

        Returns:
            pn.template.FastListTemplate: Configured template.
        """
        try:
            template = pn.template.FastListTemplate(
                title="Among Friends", accent_base_color=ACCENT, header_background=ACCENT, theme_toggle=False
            )

            # Setup buttons and event handlers
            self.setup_template_buttons(template)

            # Set the initial content
            template.main.append(self.main_content)
            return template
        except Exception as e:
            raise Exception(f"Error setting up the main template: {e}")

    def setup_template_buttons(self, template: pn.template.FastListTemplate) -> None:
        """
        Setup buttons and their event handlers on the provided template.

        Args:
            template (pn.template.FastListTemplate): The template where the buttons should be added.
        """
        try:
            buttons: List[Tuple[str, callable]] = [
                ("Network Analysis", self.load_network_analysis_page),
                # ("Statistics", self.load_statistics_page),
                # ("Generative Text", self.load_gen_text_page),
            ]

            for btn_name, btn_event in buttons:
                btn = pn.widgets.Button(name=btn_name, width_policy="max")
                btn.on_click(btn_event)
                template.sidebar.append(btn)
        except Exception as e:
            raise Exception(f"Error setting up template buttons: {e}")

    def load_network_analysis_page(self, event: Any) -> None:
        """
        Load the network analysis page into the main content.

        Args:
            event (Any): Event triggering this method.
        """
        try:
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
        except Exception as e:
            raise Exception(f"Error loading network analysis page: {e}")

    def load_statistics_page(self, event: Any) -> None:
        """
        Load the statistics page into the main content.

        Args:
            event (Any): Event triggering this method.
        """
        try:
            self.main_content.clear()
            self.main_content.append(StatisticsPage().view())
        except Exception as e:
            raise Exception(f"Error loading statistics page: {e}")

    def load_gen_text_page(self, event: Any) -> None:
        """
        Load the generative text page into the main content.

        Args:
            event (Any): Event triggering this method.
        """
        try:
            self.main_content.clear()
            self.main_content.append(GenerativeText().view())
        except Exception as e:
            raise Exception(f"Error loading generating text page: {e}")

    def serve(self) -> None:
        """
        Serve the initialized template.
        """
        try:
            pn.serve(self.template)
        except Exception as e:
            raise Exception(f"Error serving the tempalte: {e}")


@click.command()
@click.option('--data-source', default="production_data", type=click.Choice(['production_data', 'mocked_data']),
              help='Data source directory: production_data or mocked_data.')
def main(data_source: str):
    app_manager = AppManager(data_source)
    app_manager.serve()


if __name__ == "__main__":
    main()
