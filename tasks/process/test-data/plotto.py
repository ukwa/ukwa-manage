from plotly.offline import plot
from plotly.graph_objs import Scatter, Figure, Layout

graph = plot([Scatter(x=[1, 2, 3], y=[3, 1, 6])], output_type='div', include_plotlyjs=True, auto_open=False, show_link=False)

print(graph[:100])

