# ---
# title: Iterative Computations
# description: An article exploring concepts related to iterative computation in Pathway.
# date: '2023-12-28'
# thumbnail: ''
# tags: ['tutorial', 'engineering']
# keywords: ['iterate', 'iteration', 'iterative', 'updates', 'connected components', 'collatz conjecture', 'newton method']
# notebook_export_path: notebooks/tutorials/iterate.ipynb
# ---

# %% [markdown]
# # Iterative Computations
# Pathway allows you to perform iterative computations on tables. The tables can be updated over time but the data is still split into batches properly after performing an iteration.
#
# The iteration is performed until convergence or until the limit of steps is reached (by default there is no limit but you can specify it). The iteration is incremental - the next iteration step only gets elements that have been updated in the previous step. It allows Pathway to reduce the amount of computations performed and effectively have a different number of iteration steps for different elements.
#
# You could replace the iteration by unrolling the computations using, e.g. a `for` loop, but then you would get the same amount of steps for each element and possibly a lot more computations performed.

# In the sections below you can find the example applications of iteration in Pathway with a more detailed explanation of how this mechanism works.

# %% [markdown]
# ## Collatz conjecture
# There is a hypothesis in mathematics that the function presented below applied iteratively to any positive integer eventually transforms it to 1. The function is
#
# $f(n) = \begin{cases} n/2  & n \text{ is even} \\ 3n+1 & n \text{ is odd} \end{cases}$
#
# The sequence is $a_0=n$, $a_i = f(a_{i-1})$ for $i > 0$ and the conjecture says that for every positive integer $n$ there exists such $i$ that $a_i=1$. You can read more about it on [Wikipedia](https://en.wikipedia.org/wiki/Collatz_conjecture).
#
# You will now learn how to use Pathway to test the Collatz conjecture. Let's import Pathway first and define a function that performs a single step of the iterative computation. It can return a single table, a tuple or a dictionary of tables.
# %%
import pathway as pw


def collatz_step(t: pw.Table) -> pw.Table:
    t = t.with_columns(
        a=pw.if_else(
            pw.this.a == 1,
            1,
            pw.if_else(pw.this.a % 2 == 0, pw.this.a // 2, 3 * pw.this.a + 1),
        )
    )
    # t.debug("collatz")
    return t


# %% [markdown]
# Let's define a table with initial numbers to be checked and apply `collatz_step` iteratively on them.

# %%
table = pw.debug.table_from_markdown(
    """
n
3
13
42
"""
).with_columns(a=pw.this.n)

res = pw.iterate(collatz_step, t=table)
pw.debug.compute_and_print(res)

# %% [markdown]
# The main part of the computation is the [`pw.iterate`](/developers/api-docs/pathway#pathway.iterate) function. It takes a function as an argument - the `collatz_step` in this case. You need to pass all tables required by `collatz_step` as keyword arguments of `pw.iterate`. In general, `pw.iterate` can return a single table, a tuple or a named tuple of tables. It depends on what is returned from the user-defined function passed to `pw.iterate`. In this case, the `collatz_step` returns a single table and that's why `pw.iterate` also returns a single table.
#
# As for the mechanism, inside the iteration records of the returned table `t` that have changed their values are passed to the beginning of the function and flow through the dataflow in `collatz_step` again as new records of `t`. The `iteration_limit` argument of `pw.iterate` was not set. That's why the computation continues until records stop updating. If the Collatz conjecture was not true for these numbers, the computation would not finish. Fortunately, it has finished, and this way we checked that it is true at least for these three numbers. You can check more numbers if you wish but don't expect anything fancy as all numbers up to $2^{69}$ have been [checked already](http://www.ericr.nl/wondrous/).
#
# If you uncomment the `t.debug("collatz")` line, you can analyze the flow of the data. You can see that the maximal number of steps was $10$ (for $n=13$) and the minimal number of steps was $8$ (for $n=3$). While counting steps, also a final step that transforms 1 to 1 was included. It is a necessary step that allows Pathway to say that nothing has changed. The `pw.iterate` can only stop if there is no change in some iteration. If there was no change in one iteration, there won't be changes in any further iterations (functions passed to `iterate` should be deterministic to avoid problems).

# %% [markdown]
# ## Newton's method
# In this example, you are going to use a [Newton method](https://en.wikipedia.org/wiki/Newton%27s_method) to find roots of a cubic polynomial. Let's consider polynomials of the form $f(x) = x^3 + ax^2 + bx + c$. Note that you can convert any third-degree polynomial for the purpose of root finding to this form by dividing by the coefficient of $x^3$. The derivative of the polynomial is $f'(x) = 3x^2 + 2ax + b$. Now you can write the formula for the solution update: $x_i = x_{i-1} - \frac{x^3 + ax^2 + bx + c}{3x^2 + 2ax + b}$ for $i > 0$ where $x_0$ is an initial guess (in the example you'll provide it by yourself). With this knowledge, let's write a code for the iteration step. In the code, a Horner's scheme is used for better speed and numerical stability.


# %%
def newton(table: pw.Table) -> pw.Table:
    f = ((pw.this.x + pw.this.a) * pw.this.x + pw.this.b) * pw.this.x + pw.this.c
    f_prime = (3 * pw.this.x + 2 * pw.this.a) * pw.this.x + pw.this.b
    table = table.with_columns(x=pw.this.x - f / f_prime)
    # table.debug("newton")
    return table


# %% [markdown]
# Let's define a table with polynomials' coefficients and starting points. There are three different polynomials in total.
# %%
table = pw.debug.table_from_markdown(
    """
 a |  b |  c | x0
-2 |  1 |  1 |  0
-2 |  1 |  1 |  2
-1 | -2 |  1 |  0
-1 | -2 |  1 |  1
 1 | -3 | -1 | -1
 1 | -3 | -1 |  0
 1 | -3 | -1 |  1
"""
).with_columns(x=pw.cast(float, pw.this.x0))

# %% [markdown]
# ![Graphs](assets/content/tutorials/iterate/poly.svg)
# <!-- https://drive.google.com/file/d/1tVghJhvq-Jwmi9BZtsnjr-b9B3Z8guj6/view?usp=sharing -->

# %% [markdown]
# The table and iteration step are defined, so you can set up a computation and print the results.

# %%
result = pw.iterate(newton, table=table)
pw.debug.compute_and_print(result)

# %% [markdown]
# By using the fact that the floating point numbers have finite precision, you don't need to specify the `iteration_limit`. If you were running some computations with slower convergence, specifying it could have been useful. Note that also a Newton method may sometimes diverge and hence never finish if you choose a wrong starting point.
#
# You can analyze the example by uncommenting the `table.debug("newton")` line. You will then see that the maximal number of steps was $17$. This number of steps was required by $x^3 - 2x^2 + x + 1 = 0$ for $x_0 = 2$. It was not a good starting point. As a result, the root estimate was changing a lot. All other computations have finished in at most $8$ steps.

# %% [markdown]
# ## Connected components in a graph
# In all cases you've seen so far, the rows were independent from each other. Those cases can be solved easily without `pw.iterate` - for example by writing a [Python UDF](/developers/api-docs/pathway#pathway.udf) that would get values from a single row and process them until convergence. For longer computations, a UDF would be slower as `pw.iterate` is implemented natively in Rust. Nevertheless, it'd be possible to do it that way.
#
# Now you'll see how to find [connected components](https://en.wikipedia.org/wiki/Component_(graph_theory)) in a graph using `pw.iterate`. Doing this is not possible with a Python UDF when each row represents a vertex or an edge in a graph. One way of solving the problem is to find for each vertex the lowest vertex label among vertices it has a path to. Let's call this vertex with the lowest label a representative. If the vertices' labels are different, each vertex in a single connected component has the same representative, and vertices from different connected components have different representatives. Then it is easy to check if the vertices belong to the same connected component.
#
# Let's create an example graph - a chain of six vertices and a single vertex. A `repr` column is also created. It contains a representative of a connected component, initially set to the vertex itself. The labels of vertices are different and hence can be used to create IDs of rows in a table by using `with_id_from`. It is needed later to have a deterministic way of assigning IDs to records.

# %%
vertices = pw.debug.table_from_markdown(
    """
    a
    1
    2
    3
    4
    5
    6
    7
""",
    id_from=["a"],
).with_columns(repr=pw.this.a)

edges = pw.debug.table_from_markdown(
    """
    a | b
    2 | 3
    3 | 4
    4 | 5
    5 | 6
    6 | 1
"""
)

# %% [markdown]
# The edges are undirected but it is easier to use them in a directed fashion. For this purpose, reversed edges are added to the graph. Also self-loops are useful.

# %%
edges = pw.Table.concat_reindex(
    edges,
    edges.select(a=pw.this.b, b=pw.this.a),
    vertices.select(a=pw.this.a, b=pw.this.a),
)

# %% [markdown]
# Now you can define an iteration step. In each step for each vertex, the representatives of neighbors are checked and the lowest label among them is be chosen as a new representative. Self-loops are needed to include the current representative in the computation. It'd be possible to preserve it differently, but this way it is cleaner. The process ends when there are no new updates - which means that all neighbors have the same representative assigned.


# %%
def cc(vertices: pw.Table, edges: pw.Table) -> pw.Table:
    edges_with_repr = edges.join(vertices, edges.b == vertices.a).select(
        edges.a, vertices.repr
    )
    vertices_updated = edges_with_repr.groupby(pw.this.a).reduce(
        pw.this.a, repr=pw.reducers.min(pw.this.repr)
    )
    # vertices_updated.debug("vertices")
    vertices_updated = vertices_updated.with_universe_of(vertices)
    return vertices_updated


# %% [markdown]

# In an iteration step, the `edges` table is joined with the `vertices` table to get the representatives of neighbors in the graph. Then `groupby` is performed on `edges_with_repr` to get a minimal representative for each vertex. A new ID is assigned based on column `a` - vertex label. It is assigned in exactly the same way it is done above when creating a table. It allows you to have the same set of keys in the `vertices_updated` table as in the `vertices` table. However, Pathway is not that clever to deduce that the keys are exactly the same in these two tables. That's why it has to be additionaly told they are the same, by using [`with_universe_of`](/developers/api-docs/pathway-table#pathway.Table.with_universe_of).

# Preserving the set of keys is important in `iterate`. The iteration can stop only stop if there are no updates in any of the records. The records correspondence between iterations is determined using their IDs. If a record with one ID disappears and a record with a new ID appears, Pathway decides that something is still changing and the computation has to continue (even if the contents of the two rows are the same). It is possible to change the set of keys used in `iterate` but in the end the set of keys has to stop changing anyway. You can see that in the next example on computing shortest distances in a graph.

# %% [markdown]
# You can now start the computations. Note that you pass two tables to `pw.iterate` but only one is updated (and hence returned from `pw.iterate`). The `edges` table is an auxiliary table that only helps to perform the computations and `pw.iterate` doesn't change the table's content. However, the `edges` table can also change its content (when it is created by a streaming source). Then the updates to `edges` result in updates inside iteration (you can see an example later in this tutorial).

# %%
result = pw.iterate(cc, vertices=vertices, edges=edges)
pw.debug.compute_and_print(result)

# %% [markdown]
# Vertices from $1$ to $6$ got $1$ as their representative and hence they belong to the same connected component. Vertex $7$ has $7$ as a representative and is a one-vertex connected component.
#
# As in the previous examples, you can uncomment the `vertices_updated.debug("vertices")` line to see how the computations were performed. You can also see the updates on a diagram below. Note that not all vertices update at once and that the vertex that was not updated for a few iterations may start being updated (it applies for example to vertex $3$).
# ![Connected components representatives updates](assets/content/tutorials/iterate/iterate_cc_1.svg)
# <!-- https://docs.google.com/drawings/d/1V68phbyysDOnlN83ObE_idMmgG2EheS49554zih_WIM/edit?usp=sharing -->

# %% [markdown]
# Now let's consider a graph that is updated over time and compute its connected components. The graph is presented below with deletions marked using red color and insertions using green color. Each event has a time associated with it. The initial vertices (at time $2$) are black-colored.
# ![Graph with online updates](assets/content/tutorials/iterate/iterate_cc_2.svg)
# <!-- https://docs.google.com/drawings/d/1oUQj2YZCQ20fg4GxeVTGoC368CQzLB4jXVpCm9Ejnig/edit?usp=sharing -->
# You can simulate a stream by adding `__time__` and `__diff__` columns to [`table_from_markdown`](/developers/api-docs/debug#pathway.debug.table_from_markdown). The `__time__` column simulates the time the record arrives to Pathway and the `__diff__` determines whether the record is inserted (`+1`, the default) or deleted (`-1`).

# %%
vertices = pw.debug.table_from_markdown(
    """
     a | __time__
     1 |     2
     2 |     2
     3 |     2
     4 |     2
     5 |     2
     6 |     2
     7 |     2
     8 |     2
     9 |     6
    10 |     8
""",
    id_from=["a"],
).with_columns(repr=pw.this.a)

edges = pw.debug.table_from_markdown(
    """
    a |  b | __time__ | __diff__
    1 |  2 |     2    |     1
    2 |  3 |     2    |     1
    2 |  8 |     2    |     1
    8 |  4 |     2    |     1
    8 |  5 |     2    |     1
    8 |  6 |     2    |     1
    4 |  7 |     2    |     1
    2 |  8 |     4    |    -1
    9 | 10 |     8    |     1
    3 |  9 |    10    |     1
    2 |  3 |    12    |    -1
""",
    id_from=["a", "b"],
)
edges = pw.Table.concat_reindex(
    edges,
    edges.select(a=pw.this.b, b=pw.this.a),
    vertices.select(a=pw.this.a, b=pw.this.a),
)

# %% [markdown]
# You can now run the computations. To see the updates over time [`pw.debug.compute_and_print_update_stream`](/developers/api-docs/debug#pathway.debug.compute_and_print_update_stream) was used. Apart from ordinary columns, it returns `__time__` and `__diff__` columns which say respectively, when the record was produced and whether it is an insertion or a deletion.

# %%
result = pw.iterate(cc, vertices=pw.iterate_universe(vertices), edges=edges)
pw.debug.compute_and_print_update_stream(result)

# %% [markdown]
# Initially (at time $2$), all vertices have the same representative ($1$) and hence belong to a single connected component. At time $4$ the edge $2-8$ is deleted and vertices from $4$ to $8$ get a new representative. At time $6$ a new node $9$ is added. It is a new connected component. At time $8$ a new node $10$ is added along with an edge $9-10$. As a result, $10$ gets $9$ as a representative. At time $10$ a new edge $3-9$ is added. In effect, $1$ is propagated as a representative of $9$ and $10$. They are in the same connected component as $1, 2, 3$. However, at time $12$ the edge $2-3$ is removed. The connected component represented by $1$ is split into two components - one with $1$ as a representative and one with $3$ as a representative.

# %% [markdown]
# ## Shortest paths in a graph
# Another interesting use of `pw.iterate` is computing shortest paths in a graph. In this example, you are going to find the shortest paths from one vertex to all accessible vertices in a graph. To demonstrate the ability of `pw.iterate` to work on tables with changing sets of rows, the initial answer only contains one vertex - a starting point. It grows when paths to new vertices are found. The graph is finite so the set of vertices finally stops growing and the distances finally stop updating so the iteration can finish.

# To find the shortest paths, the [Bellman-Ford algorithm](https://en.wikipedia.org/wiki/Bellman%E2%80%93Ford_algorithm) is used. The example graph does not contain negative cycles. If it did, the iteration would not stop. To solve the problem, you could limit the number of iterations to `#vertices-1` and check if one more step does any updates to the distances. If it does, it means that there is a negative cycle in the graph.

# Let's define the graph. In contrast to the connected components example, this time the edges are directed. They are directed from `a` to `b`.
# ![A graph with weighted edges](assets/content/tutorials/iterate/iterate_bellman.svg)
# <!-- https://docs.google.com/drawings/d/1vUz_PByX3cx97dRmi1IQxzwWo07nvkjN37guNeW95Kk/edit?usp=sharing -->

# %%

vertices = pw.debug.table_from_markdown(
    """
    a | dist
    1 |   0
""",
    id_from=["a"],
)

edges = pw.debug.table_from_markdown(
    """
    a | b | weight
    1 | 2 |    2
    2 | 3 |    3
    3 | 2 |    2
    1 | 3 |   10
    2 | 5 |    5
    3 | 4 |    3
    4 | 5 |   -2
    5 | 3 |    1
    4 | 8 |    4
    4 | 1 |   -4
    6 | 7 |    2
    7 | 6 |    3
"""
)


edges = pw.Table.concat_reindex(
    edges, vertices.select(a=pw.this.a, b=pw.this.a, weight=0)
)

# %% [markdown]
# To make the updates easier, self-loops with weight zero are added. Thanks to them, there is no need to compare distances computed in the previous and the current iteration (the only vertex that would need them is the starting vertex).
#
# In a single iteration step, for each vertex, the length of paths via each of its neighbors is computed and the smallest one is chosen. Computing path length from the starting point via each neighbor is done using a [`join`](/developers/api-docs/pathway-table#pathway.Table.join) and taking the minimal length is done by a [`groupby`](/developers/api-docs/pathway-table#pathway.Table.groupby), [`reduce`](/developers/api-docs/pathway#pathway.GroupedTable.reduce) pair.


# %%
def bellman_ford(vertices: pw.Table, edges: pw.Table) -> pw.Table:
    edges_with_dist = edges.join(vertices, edges.a == vertices.a).select(
        a=edges.b, dist=edges.weight + vertices.dist
    )
    vertices_updated = edges_with_dist.groupby(pw.this.a).reduce(
        pw.this.a, dist=pw.reducers.min(pw.this.dist)
    )
    # vertices_updated.debug("vertices")
    return vertices_updated


# %% [markdown]
# Now you can use the `bellman_ford` in iterate. Note the [`pw.iterate_universe`](/developers/api-docs/pathway#pathway.iterate_universe) wrapper for the `vertices` table. It is needed to tell `pw.iterate` that the vertices table changes its set of IDs.

# %%
res = pw.iterate(bellman_ford, vertices=pw.iterate_universe(vertices), edges=edges)
pw.debug.compute_and_print(res)

# %% [markdown]
# As in the previous examples, you can uncomment `vertices_updated.debug("vertices")` to see the flow of data inside iteration. Also, note that the final result does not contain distances to vertices $6$ and $7$. It is caused by the fact that the initial set only contains the starting vertex and only the accessible (in the same connected component) vertices are added to it. Vertices $6$ and $7$ form a separate connected component.
#
# If you wish, you can try simulating changes in the graph over time by adding `__time__` and `__diff__` columns to `pw.debug.table_from_markdown`. Then you can see the changes of the result over time by using `pw.debug.compute_and_print_update_stream(res)` instead of `pw.debug.compute_and_print(res)`. If you have problems, see how it was implemented for the connected components above.

# %% [markdown]
# ## Summary
# In this tutorial, you have learned how the iteration works in Pathway and what are its applications. It can be useful in problems you encounter as well!
