import itertools as it
import networkx as nx
import matplotlib.pyplot as plt


def draw_labeled_multigraph(rels: list[dict[str, str]]) -> None:
    G = nx.MultiDiGraph()
    for rel in rels:
        G.add_edge(rel["start"], rel["end"], label=f"{rel['label']}[{rel['position']}]")

    style = [f"arc3,rad={r}" for r in it.accumulate([0.15] * 4)]

    pos = nx.shell_layout(G)
    nx.draw_networkx_nodes(G, pos)
    nx.draw_networkx_labels(G, pos, font_size=10)
    nx.draw_networkx_edges(G, pos, edge_color="grey", connectionstyle=style)

    labels = {
        tuple(edge): attrs["label"]
        for *edge, attrs in G.edges(keys=True, data=True)
    }
    nx.draw_networkx_edge_labels(
        G,
        pos,
        labels,
        connectionstyle=style,
        label_pos=0.3,
        font_color="blue",
        bbox={"alpha": 0},
    )
    plt.savefig("graph.jpeg")
