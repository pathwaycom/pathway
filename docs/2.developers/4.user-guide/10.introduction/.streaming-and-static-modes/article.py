# ---
# title: "Streaming and Static Modes"
# description: ''
# ---

# # Streaming and Static Modes
# While Pathway is made for processing bounded and unbounded streaming data, entirely static data can also be used for testing and debugging purposes. This article explains what are those two modes -streaming and static- and their differences.
#
# Pathway is purposely designed to work with streaming data.
# However, working in the **streaming mode** may not be the most convenient way to test and debug your application.
# To ease testing and debugging, Pathway provides a **static mode** which allows you to manipulate static and finite data.
# In the following, you are going to see what lies behind those terms, their differences, and how to use both modes.
#
# ## Dataflow
#
# Let's start with a brief explanation of Pathway's [dataflow](/developers/user-guide/introduction/concepts#dataflow).
# In Pathway, the processing pipeline that you define is modeled using a graph.
# This graph, called the dataflow, models the different transformation steps performed on the data.
# Each node is either an operation performed on one or more tables (e.g. a transformation), or a connector.
#
#
# For example, consider the following scenario: you have two tables containing the ages (in T1) and the countries (in T2) of different people and you want to compute the list of people from a given country (let's say the US) and the list of people in this country and whether they are adults or not.
# In Pathway, you can build a function which works on two tables T1 and T2 as follows:

# ```python
# T1bis = T1.select(*pw.this, adult=pw.apply(lambda x: x>18, pw.this.age))
# T2bis = T2.filter(pw.this.country == "US")
# T3 = T1bis.join(T2bis, pw.left.name == pw.right.name).select(
#     pw.left.name, pw.left.adult
# )
# ```

# In practice, you would need two input connectors to create T1 and T2, you can use the previous sample of code to build T2bis and T3, and then use output connectors to output the resulting tables outside of Pathway.
# The dataflow of such a pipeline would be looking like this:
#
# ![Universe](/assets/content/documentation/graph.svg)
#
# The corresponding notations would be: T1' $\rightarrow$ T1bis, T2' $\rightarrow$ T2bis, t1 $\rightarrow$ `select` with `apply`, t2 $\rightarrow$ `filter`, and t3 $\rightarrow$ `join` and `select`.
#
# The main take-away is that Pathway builds this graph based on the pipeline defined by the user, before any computation is actually done.
# This graph models the relations between the different connectors, tables, and transformations.
# Its main purpose is to provide fast updates in streaming mode.
#
#
# ## Streaming mode
# In the **streaming mode**, Pathway assumes unbounded input data updates.
# This mode requires input connectors listening to streaming data sources.
# Pathway starts to ingest the data into the dataflow only when `pw.run()` is called and the data is maintained in the graph during the computation: results cannot be printed but should be accessed using output connectors.
# The computation runs until the process is killed: everything after `pw.run()` is unreachable code.
#
#
# In this example, T1 and T2 could be obtained using Kafka connectors, and T2bis and T3 could be output using PostgreSQL connectors.
#
# Pathway uses the graph to provide fast updates: instead of ingesting the data from scratch in the graph everytime an update is received, the data is maintained in the graph and locally updated.
# For instance, the reception of an update from the Input Data 1, would only modify T1, T1', and T3 without any impact on T2 and T2'.
# The updates are processed as they are received, without notion of batch, providing realtime streaming processing.
#
# Pathway is designed to be run in this streaming mode and it is the standard way to run Pathway.
#
# ## Static mode
# As briefly mentioned, the streaming mode may not be the most convenient when testing or debugging.
# For that purpose, Pathway provides a **static mode** in which static data may be attached to the connectors.
# In that mode, finite and static data can be loaded, e.g. from a table written in a static csv file or from a markdown table.
#
# In our example, Input Data 1 and Input Data 2 can be small static tables written by hand in a markdown file.
# Pathway provides static output connectors to load this static data:
#
# ```python
# T1 = pw.debug.table_from_markdown(
#     """
#     | name  | age
#  1  | Alice | 15
#  2  | Bob   | 32
#  3  | Carole| 28
#  4  | David | 35
#  """)
# T2 = pw.debug.table_from_markdown(
#     """
#     | name  | country
#  1  | Alice | US
#  2  | Bob   | France
#  3  | Carole| Germany
#  4  | David | US
#  """)
# ```
#
# When the computation is run in the static mode, all the data is loaded and processed at once.
# While the static mode does not fully benefit from the dataflow, it allows checking if the graph is correctly built.
#
# To ease the debugging, Pathway provides a function called `compute_and_print`.
# When calling `pw.debug.compute_and_print(t)`, Pathway builds the whole graph, ingests all the available static data, prints the obtained table `t`, and then discards the data.
# Calling twice `compute_and_print` will result in ingesting the data twice.

# ## Summaries of differences
#
# Here is a summary of the main differences in the two modes:
#
# |                                | **Streaming mode**                                | **Static mode** |
# | ------------------------------ | :-----------------------------------------------: | :-------------: |
# | Data type                      | unbounded, streaming data                         | finite, static data |
# | Computation type               | streaming                                         | batch |
# | Can be used in production      | yes                                               | no |
# | Starting the computation       | `pw.run`                                          | `pw.debug.compute_and_print` or `pw.run` |
# | Builds a dataflow              | yes                                               | yes |
# | How the data is ingested in the graph      | maintained at each update                         | ingested from scratch everytime |
# | Termination of the computation | runs forever          | automatically terminated once the graph is built |
# | Printing data                  | no, data should be accessed using output connectors | yes, via `pw.debug.compute_and_print` |

# ## Comparative example
#
# In the following, we implement our full example into Pathway using both modes.
#
# ### Common pipeline
#
# The processing pipeline should be designed in the same way no matter what mode is used in the end. The only difference should be with regartds to how the input and output data is manipulated.
# To highlight the fact that rest of the implementation remains the same, you can implement the pipeline in a function taking T1 and T2 as parameters and returning T2bis and T3:

# +
import pathway as pw


class SchemaT1(pw.Schema):
    name: str
    age: int


class SchemaT2(pw.Schema):
    name: str
    country: str


def pipeline(T1, T2):
    T1bis = T1.select(*pw.this, adult=pw.apply(lambda x: x > 18, pw.this.age))
    T2bis = T2.filter(pw.this.country == "US")
    T3 = T1bis.join(T2bis, pw.left.name == pw.right.name).select(
        pw.left.name, pw.left.adult
    )
    return (T2bis, T3)


# -

# ### Streaming mode
#
# In the streaming mode, we must connect to external data sources such as CSV files for the input and for the output.
# Our implementation would be like this:
#
# ```python
# T1=pw.io.csv.read(inputDir1, schema=SchemaT1, mode="streaming")
# T2=pw.io.csv.read(inputDir2, schema=SchemaT2, mode="streaming")
# T2bis,T3=pipeline(T1,T2)
# pw.io.csv.write(T2bis, outputDir1)
# pw.io.csv.write(T3, outputDir2)
# pw.run()
# ```
#
# The computation is started with `pw.run()` and will not finish until the process is killed.
# The results of the computations are sent to PostgreSQL via the output connectors.

# ### Static mode
#
# In the static mode, if the connectors are compatible, we only need to change the mode from "streaming" to "static":
#
# ```python
# T1=pw.io.csv.read(inputDir1, schema=SchemaT1, mode="static")
# T2=pw.io.csv.read(inputDir2, schema=SchemaT2, mode="static")
# T2bis,T3=pipeline(T1,T2)
# pw.io.csv.write(T2bis, outputDir1)
# pw.io.csv.write(T3, outputDir2)
# pw.run()
# ```
#
# And that's it!
# The computation will be launched, all the available data will be ingested and processed,
# and the output will be written in the same CSV files.
# The only difference is that the files will not be updated whenever new CSV files are added to the input directories:
# only the data available at launch time will be processed.
#
# But the static mode has more to offer: we can enter the input data by hand and check step by step what is happening on this data.
# With manually entered data, our example becomes:

T1 = pw.debug.table_from_markdown(
    """
    | name  | age
 1  | Alice | 15
 2  | Bob   | 32
 3  | Carole| 28
 4  | David | 35
 """
)
T2 = pw.debug.table_from_markdown(
    """
    | name  | country
 1  | Alice | US
 2  | Bob   | France
 3  | Carole| Germany
 4  | David | US
 """
)
T2bis, T3 = pipeline(T1, T2)

# Without output connectors, this implementation does not do any computation: it builds the graph but does not add any data.
# We need to trigger the data insertion into the graph by printing some data.
# In the static mode, we can check that our tables have been well defined and loaded by printing them:

pw.debug.compute_and_print(T1)

# The extra column contains the indexes of the rows (the `...` at the end means that they are truncated for display – the full IDs are 128 bit and are a bit long in a text format 😉).
# Every table has such a column and `id` is a reserved name which cannot be used as column name.
# Indexes are pointers and can be generated based on a given input column, otherwise they are generated automatically.

pw.debug.compute_and_print(T2)

# You can also check that the pipeline returns the expected tables:

pw.debug.compute_and_print(T2bis)

pw.debug.compute_and_print(T3)

# ## Conclusion
#
# While Pathway is made for the streaming mode, the static mode can be used to test and debug your pipeline.
# The implementation should be the same in both modes. Only the way data is input and output differs.
#
# The different ways to access the data both in streaming and static mode is explained in more details in our [guide to connectors](/developers/user-guide/connect/pathway-connectors/).
