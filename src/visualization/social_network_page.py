import panel as pn


class SocialNetworkPage:
    def __init__(
        self, date_range_slider, node_selector, min_date, max_date, ui_components
    ):
        """
        Initializes the SocialNetworkPage class.

        Args:
            date_range_slider: The widget to select the date range for filtering data.
            node_selector: The widget to select a particular node for visualization.
            min_date: Minimum date for the date range slider.
            max_date: Maximum date for the date range slider.
            ui_components: The UI components utility class containing methods to generate visual components.

        Raises:
            Exception: If there's an error while initializing the class components.
        """
        try:
            self.date_range_slider = date_range_slider
            self.node_selector = node_selector
            self.ui_components = ui_components

            # Initialize components
            self.graph_pane = self.initialize_graph_pane()
            self.closeness_table = self.initialize_closeness_table(min_date, max_date)
            self.sidebar = self.initialize_sidebar()
            self.content = self.initialize_content()

            # Set up watchers to update views when widget values change
            self.setup_watchers()
        except Exception as e:
            raise Exception(f"Error encountered while initializing SocialNetworkPage: {e}")

    def initialize_graph_pane(self):
        """
        Initializes and returns the graph pane.

        Returns:
            pn.pane.HoloViews: A HoloViews pane displaying the network graph.

        Raises:
            Exception: If there's an error while generating the graph pane.
        """
        try:
            return pn.pane.HoloViews(
                self.ui_components.get_network_graph(
                    self.node_selector.value, self.date_range_slider.value
                )
            )
        except Exception as e:
            raise Exception(f"Error encountered while initializing graph pane: {e}")

    def initialize_closeness_table(self, min_date, max_date):
        """
         Initializes and returns the closeness table for a given date range.

         Args:
             min_date: Minimum date for the date range.
             max_date: Maximum date for the date range.

         Returns:
             pn.widgets.Tabulator: A table widget displaying closeness rankings.

         Raises:
             Exception: If there's an error while generating the closeness table.
         """
        try:
            common_table_options = dict(
                page_size=10,
                pagination="remote",
                selectable=True,
                show_index=False,
                width_policy="max",
            )
            editors = {
                "Participant": {"type": "editable", "value": False},
                "Closeness Ranking": {"type": "editable", "value": False},
            }
            closeness_table_data = self.ui_components.closeness_ranking_for_node(
                (min_date, max_date), self.node_selector.value
            )
            return pn.widgets.Tabulator(
                value=closeness_table_data, editors=editors, **common_table_options
            )
        except Exception as e:
            raise Exception(f"Error encountered while initializing closeness table: {e}")

    def initialize_sidebar(self):
        """
        Initializes and returns the sidebar.

        Returns:
            pn.Column: A column layout containing widgets for user interaction.

        Raises:
            Exception: If there's an error while generating the sidebar.
        """
        try:
            return pn.Column(
                self.date_range_slider,
                self.ui_components.update_start_date_label,
                self.ui_components.update_end_date_label,
                self.node_selector,
                self.ui_components.update_eigenvector_ranking_label,
                sizing_mode="stretch_height",
            )
        except Exception as e:
            raise Exception(f"Error encountered while initializing sidebar: {e}")

    def initialize_content(self):
        """
        Initializes and returns the main content layout.

        Returns:
            pn.Row: A row layout containing the sidebar, graph, and closeness table.

        Raises:
            Exception: If there's an error while generating the content layout.
        """
        try:
            closeness_table_title = pn.pane.Markdown(
                "### Co-Participant Closeness Rankings"
            )
            closeness_table_layout = pn.Column(
                closeness_table_title, self.closeness_table, sizing_mode="stretch_width"
            )
            return pn.Row(
                self.sidebar,
                self.graph_pane,
                closeness_table_layout,
                sizing_mode="stretch_width",
            )
        except Exception as e:
            raise Exception(f"Error encountered initializing content: {e}")

    def setup_watchers(self):
        """
        Sets up event watchers to update views based on user interactions.

        Raises:
            Exception: If there's an error while setting up watchers.
        """
        try:
            self.node_selector.param.watch(self.update_closeness_table, "value")
            self.date_range_slider.param.watch(self.update_closeness_table, "value")
            self.date_range_slider.param.watch(self.update_graph_pane, "value")
            self.node_selector.param.watch(self.update_graph_pane, "value")
        except Exception as e:
            raise Exception(f"Error encountered while setting up watchers: {e}")

    def update_graph_pane(self, event):
        """
        Updates the displayed graph based on user interactions.

        Args:
            event: The event trigger.

        Raises:
            Exception: If there's an error while updating the graph pane.
        """
        try:
            new_graph = self.ui_components.get_network_graph(
                self.node_selector.value, self.date_range_slider.value
            )
            self.graph_pane.object = new_graph
        except Exception as e:
            raise Exception(f"Error encountered while updating graph pane: {e}")

    def update_closeness_table(self, event):
        """
        Updates the closeness table based on user interactions.

        Args:
            event: The event trigger.

        Raises:
            Exception: If there's an error while updating the closeness table.
        """
        try:
            new_data = self.ui_components.closeness_ranking_for_node(
                self.date_range_slider.value, self.node_selector.value
            )
            self.closeness_table.value = new_data
        except Exception as e:
            raise Exception(f"Error encountered while updating closeness table: {e}")

    def view(self):
        """
        Returns the main content layout to be displayed.

        Returns:
            pn.Row: The main content layout.
        """
        return self.content
