import panel as pn


class SocialNetworkPage:
    def __init__(
        self, date_range_slider, node_selector, min_date, max_date, ui_components
    ):
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

    def initialize_graph_pane(self):
        return pn.pane.HoloViews(
            self.ui_components.get_network_graph(
                self.node_selector.value, self.date_range_slider.value
            )
        )

    def initialize_closeness_table(self, min_date, max_date):
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

    def initialize_sidebar(self):
        return pn.Column(
            self.date_range_slider,
            self.ui_components.update_start_date_label,
            self.ui_components.update_end_date_label,
            self.node_selector,
            self.ui_components.update_eigenvector_ranking_label,
            sizing_mode="stretch_height",
        )

    def initialize_content(self):
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

    def setup_watchers(self):
        self.node_selector.param.watch(self.update_closeness_table, "value")
        self.date_range_slider.param.watch(self.update_closeness_table, "value")
        self.date_range_slider.param.watch(self.update_graph_pane, "value")
        self.node_selector.param.watch(self.update_graph_pane, "value")

    def update_graph_pane(self, event):
        new_graph = self.ui_components.get_network_graph(
            self.node_selector.value, self.date_range_slider.value
        )
        self.graph_pane.object = new_graph

    def update_closeness_table(self, event):
        new_data = self.ui_components.closeness_ranking_for_node(
            self.date_range_slider.value, self.node_selector.value
        )
        self.closeness_table.value = new_data

    def view(self):
        return self.content
