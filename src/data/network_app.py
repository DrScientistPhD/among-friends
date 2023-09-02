import pandas as pd
import networkx as nx
import panel as pn
import holoviews as hv
from holoviews import opts
from src.models.sna_graph_builder import SnaGraphBuilder, SnaMetricCalculator
from faker import Faker

# Extensions
hv.extension('bokeh')

# Constants and Colors
ACCENT = "#BB2649"
fake = Faker()


def fake_nodes_edges_dataframe():
    n = 100
    data = {
        "target_participant_id": [fake.random_int(min=1, max=10) for _ in range(n)],
        "target_datetime": [fake.date_this_decade() for _ in range(n)],
        "source_participant_id": [fake.random_int(min=1, max=10) for _ in range(n)],
        "source_datetime": [fake.date_this_decade() for _ in range(n)],
        "weight": [fake.random_number(digits=2) for _ in range(n)],
        "interaction_category": [fake.random_element(elements=("response", "quotation", "emoji")) for _ in range(n)]
    }
    return pd.DataFrame(data)


def filter_dataframe_by_dates(df, start_date, end_date):
    df['target_datetime'] = pd.to_datetime(df['target_datetime'])
    df['source_datetime'] = pd.to_datetime(df['source_datetime'])
    mask = (df['target_datetime'] >= start_date) & (df['target_datetime'] <= end_date)
    return df[mask]


nodes_edges_df = fake_nodes_edges_dataframe()
min_date = nodes_edges_df["target_datetime"].apply(pd.Timestamp).min()
max_date = nodes_edges_df["target_datetime"].apply(pd.Timestamp).max()

date_range_slider = pn.widgets.DateRangeSlider(
    name='Date Range Slider',
    start=min_date,
    end=max_date,
    value=(min_date, max_date)
)


@pn.depends(date_range_slider.param.value)
def generate_filtered_graph(date_range):
    start_date, end_date = date_range
    nodes_edges_filtered_df = filter_dataframe_by_dates(nodes_edges_df, start_date, end_date)
    G = SnaGraphBuilder.create_network_graph(nodes_edges_filtered_df)
    participants = list(G.nodes)
    pos = nx.spring_layout(G)
    return G, participants, pos


G, participants, pos = generate_filtered_graph((min_date, max_date))

node_selector = pn.widgets.Select(options=sorted(participants), name='Select Node')


def eigenvector_table(date_range):
    G, _, _ = generate_filtered_graph(date_range)
    eigenvector_metrics = SnaMetricCalculator.generate_eigenvector_metrics(G)
    rankings = eigenvector_metrics["Eigenvector Rank"]
    data = [{"Participant": node, "Rank": rankings[node]} for node in G.nodes()]
    df = pd.DataFrame(data)
    df = df.sort_values(by="Rank")[["Participant", "Rank"]]
    return df

# TODO: SnaMetricCalculator.generate_closeness_metrics(G) isn't returning what I planned it to return - investigate this asap
def closeness_ranking_for_node(date_range, node):
    G, _, _ = generate_filtered_graph(date_range)
    closeness_metrics = SnaMetricCalculator.generate_closeness_metrics(G)
    rankings = closeness_metrics["Closeness Ranking"][node]
    data = [{"Participant": participant, "Distance": distance} for participant, distance in rankings.items()]
    df = pd.DataFrame(data)
    df = df.sort_values(by="Distance")[["Participant", "Distance"]]
    return df


@pn.depends(node_selector.param.value, date_range_slider.param.value)
def get_graph(node_selector_value, date_range):
    G, _, _ = generate_filtered_graph(date_range)
    for node in G.nodes():
        G.nodes[node]['color'] = '#ffa07a' if node == node_selector_value else '#add8e6'

    eigenvector_metrics = SnaMetricCalculator.generate_eigenvector_metrics(G)
    eigenvector_scores = eigenvector_metrics["Eigenvector Score"]
    max_size, min_size = 50, 10
    min_eigenvector_score = min(eigenvector_scores.values())
    max_eigenvector_score = max(eigenvector_scores.values())
    denominator = max_eigenvector_score - min_eigenvector_score

    if denominator == 0:
        for node in G.nodes():
            G.nodes[node]['size'] = min_size
    else:
        for node in G.nodes():
            size = min_size + (eigenvector_scores[node] - min_eigenvector_score) / denominator * (max_size - min_size)
            G.nodes[node]['size'] = size

    graph = hv.Graph.from_networkx(G, pos).opts(
        opts.Graph(width=700, height=600, tools=['hover', 'tap'], node_size='size', edge_line_width=1, edge_alpha=0.5,
                   node_color='color', edge_color=ACCENT, xaxis=None, yaxis=None)
    )
    return graph


class SocialNetworkPage:
    def __init__(self):
        self.graph_pane = pn.panel(get_graph, width_policy='max', height=600, sizing_mode='stretch_width')

        # Common options for the tables
        common_table_options = dict(page_size=10, pagination='remote', selectable=True,
                                    show_index=False, width_policy='max', sizing_mode='stretch_width')

        # Create the Tabulator widget with initial eigenvector data
        table_data = eigenvector_table((min_date, max_date))
        editors = {
            'Participant': {'type': 'editable', 'value': False},
            'Rank': {'type': 'editable', 'value': False}
        }
        self.table = pn.widgets.Tabulator(value=table_data, editors=editors, **common_table_options)

        # Callback to update the node selector based on table selection
        def update_node_selector(event):
            if event.new:
                selected_row_index = event.new[0]  # Considering single selection
                selected_dataframe = self.table.value
                node_selector.value = selected_dataframe.iloc[selected_row_index]['Participant']

        self.table.param.watch(update_node_selector, 'selection')

        # Callback to update the eigenvector table based on date range slider value
        def update_table(event):
            new_data = eigenvector_table(event.new)
            self.table.value = new_data

        date_range_slider.param.watch(update_table, 'value')

        # Create the Tabulator widget with initial closeness data
        self.closeness_table_data = closeness_ranking_for_node((min_date, max_date), node_selector.value)
        self.closeness_table = pn.widgets.Tabulator(value=self.closeness_table_data, **common_table_options)

        # Callback to update the closeness table based on either the node selector or date range slider values
        def update_closeness_table(event):
            new_data = closeness_ranking_for_node(date_range_slider.value, node_selector.value)
            self.closeness_table.value = new_data

        node_selector.param.watch(update_closeness_table, 'value')
        date_range_slider.param.watch(update_closeness_table, 'value')

        # Create a sidebar
        sidebar = pn.Column(date_range_slider, node_selector, sizing_mode='stretch_height')

        # Modify the layout to include both tables and sidebar
        self.content = pn.Row(
            sidebar,
            self.graph_pane,
            pn.Row(self.table, self.closeness_table, sizing_mode='stretch_width'),
            sizing_mode='stretch_width'
        )

    def view(self):
        return self.content


pages = {
    "Social Network": SocialNetworkPage()
}

template = pn.template.FastListTemplate(
    title="Social Network Visualization",
    main=[pages["Social Network"].view()],
    accent_base_color=ACCENT,
    header_background=ACCENT
)

pn.serve(template)
