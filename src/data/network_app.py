import pandas as pd
import networkx as nx
import panel as pn
import holoviews as hv
from holoviews import opts
from src.models.sna_graph_builder import SnaGraphBuilder, SnaMetricCalculator
from faker import Faker
import warnings
from bokeh.models import HoverTool


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
    name='Group Chat Date Range',
    start=min_date,
    end=max_date,
    value=(min_date, max_date),
    show_value=False,
    bar_color="red"
)

@pn.depends(date_range_slider.param.value)
def update_start_date_label(date_range):
    start_date, _ = date_range
    start_date_text = f"<font size='4'>Start Date: {start_date.strftime('%b %d, %Y')}</font>"
    return pn.pane.HTML(start_date_text)

start_date_label = pn.panel(update_start_date_label, width=400)

@pn.depends(date_range_slider.param.value)
def update_end_date_label(date_range):
    _, end_date = date_range
    end_date_text = f"<font size='4'>End Date: {end_date.strftime('%b %d, %Y')}</font>"
    return pn.pane.HTML(end_date_text)

end_date_label = pn.panel(update_end_date_label, width=400)

@pn.depends(date_range_slider.param.value)
def generate_filtered_graph(date_range):
    start_date, end_date = date_range
    nodes_edges_filtered_df = filter_dataframe_by_dates(nodes_edges_df, start_date, end_date)
    G = SnaGraphBuilder.create_network_graph(nodes_edges_filtered_df)
    participants = list(G.nodes)
    pos = nx.spring_layout(G)
    eigenvector_metrics = SnaMetricCalculator.generate_eigenvector_metrics(G)
    return G, participants, pos, eigenvector_metrics

G, participants, pos, _ = generate_filtered_graph((min_date, max_date))
node_selector = pn.widgets.Select(options=sorted(participants), name='Group Chat Participant')

@pn.depends(node_selector.param.value, date_range_slider.param.value)
def update_eigenvector_ranking_label(selected_node, date_range):
    _, _, _, eigenvector_metrics = generate_filtered_graph(date_range)
    rankings = eigenvector_metrics["Eigenvector Rank"]
    if selected_node in rankings:
        rank = rankings[selected_node]
        total_participants = len(rankings)
        return f"<font size='4'>Influencer Ranking: {rank}/{total_participants}</font>"
    else:
        return "<font size='4'>Participant not found in the current date range</font>"

eigenvector_ranking_label = pn.panel(update_eigenvector_ranking_label, width=400)

def closeness_ranking_for_node(date_range, node):
    G, _, _, _ = generate_filtered_graph(date_range)
    closeness_metrics = SnaMetricCalculator.generate_closeness_metrics(G)
    rankings = closeness_metrics["Closeness Rank"].get(node, None)
    if rankings is None:
        warnings.warn(f"No closeness rankings found for node: {node}")
        return pd.DataFrame(columns=["Participant", "Closeness Ranking"])
    data = [{"Participant": participant, "Closeness Ranking": distance_rank} for participant, distance_rank in rankings.items()]
    df = pd.DataFrame(data)
    if "Closeness Ranking" in df.columns:
        df = df.sort_values(by="Closeness Ranking")[["Participant", "Closeness Ranking"]]
    else:
        warnings.warn("No 'Closeness Ranking' column found in the DataFrame.")
        df = pd.DataFrame(columns=["Participant", "Closeness Ranking"])
    return df


@pn.depends(node_selector.param.value, date_range_slider.param.value)
def get_graph(node_selector_value, date_range):
    G, _, _, eigenvector_metrics = generate_filtered_graph(date_range)

    # Normalize weights between 0 and 1
    weights = [d['weight'] for _, _, d in G.edges(data=True)]
    max_weight = max(weights)
    min_weight = min(weights)
    normalized_weights = [(w - min_weight) / (max_weight - min_weight) for w in weights]

    # Raise the normalized weights to a power to exaggerate differences
    power = 2
    adjusted_weights = [nw ** power for nw in normalized_weights]

    # Scale the adjusted normalized weights for edge thickness
    min_edge_thickness = 0.5
    max_edge_thickness = 5
    edge_widths = [min_edge_thickness + aw * (max_edge_thickness - min_edge_thickness) for aw in adjusted_weights]

    # Assign edge widths to edge data
    for (_, _, d), width in zip(G.edges(data=True), edge_widths):
        d['edge_width'] = width

    for node in G.nodes():
        G.nodes[node]['color'] = '#ffa07a' if node == node_selector_value else '#add8e6'
        rank = eigenvector_metrics["Eigenvector Rank"][node]
        total_participants = len(G.nodes())
        G.nodes[node]['influencer_ranking'] = f"{rank}/{total_participants}"

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

    # Create a Bokeh HoverTool to customize hover information
    hover = HoverTool(tooltips=[("Participant ID", "@index"), ("Influencer Ranking", "@influencer_ranking")])

    graph = hv.Graph.from_networkx(G, pos).opts(
        opts.Graph(width=700, height=600, tools=[hover, 'tap'], node_size='size',
                   edge_line_width='edge_width', edge_alpha=0.5,
                   node_color='color', edge_color=ACCENT, xaxis=None, yaxis=None)
    )

    return graph


class HelloWorldPage:
    def view(self):
        return pn.pane.Markdown("## Hello, World!")


class FoobarPage:
    def view(self):
        return pn.pane.Markdown("## Foobar!")


class SocialNetworkPage:
    def __init__(self):
        self.graph_pane = pn.panel(get_graph, width_policy='max', height=600, sizing_mode='stretch_width')

        common_table_options = dict(page_size=10, pagination='remote', selectable=True,
                                    show_index=False, width_policy='max', sizing_mode='stretch_both')

        # Applying non-editable settings to the closeness_table_data
        editors = {
            'Participant': {'type': 'editable', 'value': False},
            'Closeness Ranking': {'type': 'editable', 'value': False}
        }

        self.closeness_table_data = closeness_ranking_for_node((min_date, max_date), node_selector.value)
        self.closeness_table = pn.widgets.Tabulator(value=self.closeness_table_data, editors=editors,
                                                    **common_table_options)

        def update_closeness_table(event):
            new_data = closeness_ranking_for_node(date_range_slider.value, node_selector.value)
            self.closeness_table.value = new_data
        node_selector.param.watch(update_closeness_table, 'value')
        date_range_slider.param.watch(update_closeness_table, 'value')
        sidebar = pn.Column(node_selector, eigenvector_ranking_label, date_range_slider, start_date_label, end_date_label, sizing_mode='stretch_height')
        closeness_table_title = pn.pane.Markdown("### Co-Participant Closeness Rankings")
        closeness_table_layout = pn.Column(closeness_table_title, self.closeness_table, sizing_mode='stretch_width')
        self.content = pn.Row(sidebar, self.graph_pane, closeness_table_layout, sizing_mode='stretch_width')

    def view(self):
        return self.content

pages = {
    "Network Analysis": SocialNetworkPage(),
    "Hello World": HelloWorldPage(),
    "Foobar": FoobarPage()
}

# Create navigation tabs for each page
nav = pn.Tabs(*[(name, page.view()) for name, page in pages.items()])

# Create the template
template = pn.template.FastListTemplate(title="Among Friends", accent_base_color=ACCENT, header_background=ACCENT)

# Create buttons for the sidebar navigation
network_analysis_button = pn.widgets.Button(name='Network Analysis', width=200)
hello_world_button = pn.widgets.Button(name='Hello World', width=200)
foobar_button = pn.widgets.Button(name='Foobar', width=200)

# Append buttons to the sidebar
template.sidebar.append(network_analysis_button)
template.sidebar.append(hello_world_button)
template.sidebar.append(foobar_button)

# Create a Row to hold the main content and set the initial content
main_content = pn.Row(SocialNetworkPage().view(), sizing_mode='stretch_both')


# Update functions for the buttons
def load_network_analysis_page(event):
    main_content.clear()
    main_content.append(SocialNetworkPage().view())


def load_hello_world_page(event):
    main_content.clear()
    main_content.append(HelloWorldPage().view())


def load_foobar_page(event):
    main_content.clear()
    main_content.append(FoobarPage().view())


# Create sidebar buttons and attach event handlers
network_analysis_button = pn.widgets.Button(name="Network Analysis", min_width=200, sizing_mode='stretch_width')
hello_world_button = pn.widgets.Button(name="Hello World", min_width=200, sizing_mode='stretch_width')
foobar_button = pn.widgets.Button(name="Foobar", min_width=200, sizing_mode='stretch_width')

network_analysis_button.on_click(load_network_analysis_page)
hello_world_button.on_click(load_hello_world_page)
foobar_button.on_click(load_foobar_page)

# Create the template with the sidebar buttons and the main content
template = pn.template.FastListTemplate(
    title="Among Friends",
    sidebar=[network_analysis_button, hello_world_button, foobar_button],
    main=[main_content],
    accent_base_color=ACCENT,
    header_background=ACCENT
)

template.sidebar.sizing_mode = 'stretch_both'

pn.serve(template)