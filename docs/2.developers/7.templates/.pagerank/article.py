# ---
# title: Real-Time PageRank on Dynamic Graphs with Pathway
# description: Demonstration of a PageRank computation
# notebook_export_path: notebooks/tutorials/pagerank.ipynb
# aside: true
# article:
#   date: '2022-11-07'
#   thumbnail: '/assets/content/blog/th-computing-pagerank.png'
#   tags: ['tutorial', 'machine-learning']
# keywords: ['pagerank', 'graph', 'notebook']
# author: 'pathway'
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Real-Time PageRank on Dynamic Graphs with Pathway
#
# ## Introduction
# PageRank is best known for its success in ranking web pages in Google Search engine.
# Here is a [quick description](https://en.wikipedia.org/w/index.php?title=PageRank&oldid=1111494883):
# > PageRank works by counting the number and quality of links to a page to determine a
# > rough estimate of how important the website is. The underlying assumption is that
# > more important websites are likely to receive more links from other websites.
#
# In fact, the algorithm outputs a probability distribution that represents the
# likelihood of arriving at any particular page after randomly clicking on links for a while.
# We will simulate this behavior by the following 'surfing the Internet' procedure:
# - Initially, at each page, some amount of people start surfing the internet from that page.
# - In each turn, some users decide to click on a random link and visit a new page.
# - We iterate for a fixed number of rounds.
#
# This article assumes that you are already familiar with some basics of [Pathway transformations](/developers/user-guide/introduction/concepts#processing-the-data-with-transformations).
#
# ## Code
# First things first - imports and constants.

# %%
from typing import Any

import pathway as pw


# %% [markdown]
# ### I/O Data
# We use an `Edge` schema to represent the graph and `Result` schema to represent the final ranks.
# %%
class Edge(pw.Schema):
    u: Any
    v: Any


class Result(pw.Schema):
    rank: float


# %% [markdown]
# `pagerank` performs one turn of 'surfing the Internet' procedure by uniformly
# distributing rank from each node to all its adjacent nodes, for a fixed number of rounds.
#
# %%
def pagerank(edges: pw.Table[Edge], steps: int = 5) -> pw.Table[Result]:
    in_vertices = edges.groupby(id=edges.v).reduce(degree=0)
    out_vertices = edges.groupby(id=edges.u).reduce(degree=pw.reducers.count())
    degrees = pw.Table.update_rows(in_vertices, out_vertices)
    base = out_vertices.difference(in_vertices).select(flow=0)

    ranks = degrees.select(rank=6_000)

    grouper = edges.groupby(id=edges.v)

    for step in range(steps):
        outflow = degrees.select(
            flow=pw.if_else(
                degrees.degree == 0, 0, (ranks.rank * 5) // (degrees.degree * 6)
            )
        )

        inflows = edges.groupby(id=edges.v).reduce(
            flow=pw.reducers.sum(outflow.ix(edges.u).flow)
        )

        inflows = pw.Table.concat(base, inflows)

        ranks = inflows.select(rank=inflows.flow + 1_000).with_universe_of(degrees)
    return ranks


# %% [markdown]
# ### Tests
# We present two easy test cases here.
# A test case with a single 3-vertices loop with one backward edge.
# %%
# directed graph
vertices = pw.debug.table_from_markdown(
    """
      |
    a |
    b |
    c |
    """
).select()
edges = pw.debug.table_from_markdown(
    """
    u | v
    a | b
    b | c
    c | a
    c | b
    """,
).select(u=vertices.pointer_from(pw.this.u), v=vertices.pointer_from(pw.this.v))

pw.debug.compute_and_print(pagerank(edges))

# %% [markdown]
# Why these numbers? 3945, 6981, 7069? Feel free to skip the quite mathy explanation below.
#
# Let us calculate what the correct answer should be.
# PageRank actually finds a [stationary distribution](https://en.wikipedia.org/wiki/Markov_chain#Stationary_distribution_relation_to_eigenvectors_and_simplices)
# of a random walk on a graph in which the probability of each move depends only on the
# currently visited state, i.e. it is a Markov Chain.
#
# One may think that the transition matrix of the Markov chain in our example is
# $$
# P=\left(\begin{array}{cc}
# 0.05 & 0.9 & 0.05\\
# 0.05 & 0.05 & 0.9\\
# 0.475 & 0.475 & 0.05
# \end{array}\right)
# $$
# We move to a new page with probability 5/6 uniformly distributed among all the linked (adjacent) pages,
# and with probability 1/6 we mix uniformly at random.
# The result is a stationary distribution roughly of $(x = ( 0.215 \quad  0.397 \quad 0.388) )$ which is proportional to the rank returned.
# However, we output only the approximation of this result, and our output is not normalized.
#
# ### Summary
# As always, feel free to play and experiment with this code! In case you are looking for cool real-world
# graphs to experiment with, the [Stanford Network Analysis Project](https://snap.stanford.edu/) is an excellent source
# of reference instances, big and small.
