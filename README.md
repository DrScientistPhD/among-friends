# among-friends

This repository is geared towards the author's professional development and personal enjoyment. It's not designed for
broad use or distribution.

An interactive Panel/HoloViews app for analyzing and visualizing social network patterns in Signal group text chat data.

https://github.com/DrScientistPhD/among-friends/assets/38797267/ce4fff97-0562-49f3-9603-d12197c21ed6

## Table of Contents

- [Installation](#installation)
- [Data Preparation](#data-preparation)
- [Usage](#usage)
- [Features](#features)
- [Constructing the Network Graph](#constructing-the-network-graph)
- [Network Metrics](#network-metrics)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)
- [Contact Information](#contact-information)

## Installation

To set up the environment for this project, follow these steps:

1. **Clone the repository:**

    ```bash
    git clone https://github.com/DrScientistPhD/among-friends
    ```

2. **Navigate to the project directory:**

    ```bash
    cd among-friends
    ```

3. **Install the required dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

## Data Preparation

Before running the app, ensure you've prepared your group text chat data as follows:

### Utilizing Mocked Data

This repository contains crucial CSV files that empower users to run the "Among Friends" dashboard on their local
systems without the need to extract actual text message data from the Signal App. It's essential to understand that 
this data includes fully anonymized names and uses a limited, random assortment of sentences from each participant's 
text messages. While these files do not encompass all the fields or values found in valid Signal text message data, they
contain sufficient information to enable the dashboard's functionality.
<BR>Therefore, if you opt for the mocked data, there is no need for any additional data preparation. You can proceed
directly to the next section on launching the "Among Friends" dashboard.

### Utilizing the Signal Text Message Data

among-friends relies on text message data from the Signal messaging app. Obtaining and processing this data for the  
dashboard requires several steps.

1. Follow the instructions under "How do I enable a backup?"
   at [Signal Support](https://support.signal.org/hc/en-us/articles/360007059752-Backup-and-Restore-Messages#android_enable)
   to create a backup file for the app's text message data.
2. Transfer the backup file to a desktop, then convert it to a database file
   using [signalbackup-tools](https://github.com/bepaald/signalbackup-tools).
3. Open the database in a standard database GUI and manually extract the following tables as CSV files:
    - group_membership
    - groups
    - mention
    - message
    - reaction
    - recipient
    - remapped_recipients
4. Move the files to among-friends/data/raw.

### Generating Social Network Analysis Data for the Dashboard
With the required files saved to among-friends/data/raw, navigate to the repository's home directory in your 
terminal and run:

   ```bash
   chmod +x run_make_network_dataset
   ```
Then generate the dataset for the Network Analysis page of the dashboard with:

   ```bash
   ./run_make_network_dataset
   ```

The script supports several flags:

- `--data-source`: Specify the data source to use. Choices are 'production' or 'mocked'. Default is 'mocked'.
  
  Example:
  ```bash
  ./run_make_network_dataset --data-source mocked
  ```

- `--thread-id`: Specify the thread ID to filter by. Default is 2.
  
  Example:
  ```bash
  ./run_make_network_dataset --thread-id 2
  ```

For help and additional options:

```bash
./run_make_network_dataset --help
```
The thread_id refers to the exact group chat conversation that the network data should be constructed from, and will 
vary depending on the data contents the use extracts from Signal. The resulting file, **nodes_edges_df.csv**, is 
saved to the **among-friends/data/{mocked_data or production-data}/processed** directory for the dashboard itself.



## Starting the Dashboard

Navigate to the repository's home directory in your terminal and run:

   ```bash
   chmod +x run_among_friends_app
   ```
Then start the dashboard with:

   ```bash
   ./run_among_friends_app
   ```

The script supports one flag:

- `--data-source`: Specify the data source to use. Choices are 'production' or 'mocked'. Default is 'mocked'.
  
  Example:
  ```bash
  ./run_among_friends_app --data-source mocked

For help and additional options:

```bash
./run_make_network_dataset --help
```

Visit [http://localhost:5006](http://localhost:5006) in your web browser to interact with the app.

## Dashboard Features

- Visualize relationships and connections within the chat.
- Interactivity allowing users to zoom in on specific participants or timeframes.
- Understand analysis showcasing key influencers (Influence Ranking) and the strength of interactions between
  participants (Outward Response Rankings).

## How the Network Graph is Constructed

### Overview

The provided code constructs a directed network graph for studying interactions among chat participants. This visual
tool illustrates who communicates with whom and how frequently, offering insights into social networks, communication
patterns, and other relationship dynamics. In this graph, participants are portrayed as nodes, and their interactions as
connecting lines or edges. When two participants interact multiple times, the connecting line between them strengthens.

Three types of interactions generate edges in this network: **Text Responses**, **Quotation Responses**, and **Emoji
Reactions**. Each interaction can contribute differently to the network.

#### Text Responses

A text response follows another message, with the initial message acting as a target and the response as a source. This
reflects the directionality inherent in human conversations. Typically, one text serves both as a target and a source,
capturing the ebb and flow of group conversations. The
relationship is one to many, so that a single target
message can have up to *x* source messages, where *x* is equal to the total number of group chat participants, excluding
any
instance in which a text message author is responding to themselves.

#### Quotation Responses

This type of response involves one message directly quoting another via the Signal quotation feature. A single text
might be quoted multiple times.

#### Emoji Reaction

This captures instances where a participant assigns an emoji to a specific text message, different from simply sending
an emoji as a text response.

### Weighting Interactions

Different interaction types have varying weights. While these weights might appear arbitrary, they're chosen based on
the degree of intentionality behind each interaction type. Text Responses have a weight of 1.0, Quotation Responses 2.0,
and Emoji Reactions 1.5. Both Quotation Responses and Emoji reactions are weighted more heavility than Text 
Responses as the former have more intention and directionality to them. Quotation Responses weigh more than Emoji 
Reactions because the former demands more effort, signaling a deeper level of engagement.

### Interaction Time Penalty

A time penalty is applied to each reaction such that longer delays lead to an increasingly weak edge weight. Briefly, a
half-life value derived from the typical response time between a text and the specified reaction type is
applied to the base value of the interaction so that the weight of the edge decreases as the response time increases.
This penalty reflects the fact that the length of time that occurs between a person speaking (or sending a message) and
another person's response can be meaningful and convey various aspects of communication dynamics.

## Network Metrics

### Influence Ranking

An individual participant's **Influence Ranking** is their ranked Eigenvector Centrality score, with a rank of 1
indicating the most influential participant for that time period. As this is a directed network graph, the Eigenvector
Centrality score is not just a measure of the amount of text that participant is sending, but how strongly the other
participants are responding, and what the relative influence of those participants are. In summary, the Influence
Ranking provides an easily interpretable, and yet still more nuanced and context-aware measure of importance or
influence within a network. It considers not only the quantity of connections, but also the quality and influence of
those connections.

### Outward Response Ranking

This metric identifies how strongly, relatively, the selected participant is reacting to the other participants in the
group chat. Briefly, the total weighted sum of the interactions (edges) for the selected time period is calculated, and
then ranked among all other
participants in the network who the selected participant has at least one interaction with. Let's consider the following
example: We have selected Participant A and are provided the following Outward Response Rankings:

- Participant W - 1
- Participant D - 2
- Participant T - 3
  <br>

These rankings indicate that Participant A is reacting most strongly to Participant W (in terms of interaction type,
frequency, and response time), and the least strongly with Participant T.

## Contributing

This is a personal repository for refining my abilities and developing new skills. It is not intended as a collaborative
effort.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Thanks to [Panel](https://panel.holoviz.org/) and [HoloViews](http://holoviews.org/) for their visualization
  libraries.
- Endless gratitude to bepaald and [signalbackup-tools](https://github.com/bepaald/signalbackup-tools), which made this
  work possible.

## Contact Information

For any inquiries or feedback, please contact:

- **Name:** Raymond Pasek
- **Email:** rcpasek@gmail.com
