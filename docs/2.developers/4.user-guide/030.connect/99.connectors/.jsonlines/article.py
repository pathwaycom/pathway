# ---
# title: 'JSON Lines connectors'
# description: 'Tutorial on the Pathway JSON Lines connectors.'
# date: '2024-02-20'
# tags: ['tutorial', 'connectors']
# keywords: ['connector', 'JSON Lines', 'JSON', 'input', 'output', 'static', 'streaming']
# tech_icons: ["lets-icons:json"]
# ---

# # JSON Lines connectors
# Pathway provides connectors to read and write data streamings using JSON Lines files.
#
# [JSON Lines](https://jsonlines.org/), also called newline-delimited JSON, is a format for structured data, following three requirements:
# 1. UTF-8 Encoding.
# 2. Each Line is a valid JSON Value.
# 3. The line separator is `\n`.
#
# The suggested extension for JSON Lines files is `.jsonl`.
#
# This is a correct JSON Lines entry:
# ```
# {"key": 1, "recipient": "Bill H.", "sender": "Nancy R."}
# {"key": 2, "recipient": "Harry P.", "sender": "Hermione G."}
# {"key": 3, "recipient": "Julian S.", "sender": "Dick F."}
# ```
#
# ⚠️ JSON lines connectors work both in streaming and static modes.
# Be careful as the use of connectors differs depending on the chosen mode: see the [differences](/developers/user-guide/introduction/streaming-and-static-modes/).
#
# ## Input connector
#
# To read a file or a directory, use the [`pw.io.jsonlines.read` function](/developers/api-docs/pathway-io/jsonlines#pathway.io.jsonlines.read).
# It takes several parameters, including:
# - `path`: the path of the directory or the file to read.
# - `schema` (optional): the schema of the resulting table.
# - `mode` (optional): the mode in which the connector is used, `streaming` or `static`. Defaults to `streaming`.
#
# The connector can be used to read a directory:
# ```python
# table = pw.io.jsonlines.read("./input_directory/", schema=InputSchema)
# ```
# In this case, all the files within this directory are ingested into the table.
#
# You can choose to read a single file:
# ```python
# table = pw.io.jsonlines.read("./input_file.jsonl", schema=InputSchema)
# ```
#
# **Any file modification would be reflected in the table read: if you delete a part of a file, the respective data will be deleted from the table**.
#
# Let's consider the following example, which reads a JSON Lines file and outputs it in a CSV file using the [CSV output connector](/developers/user-guide/connect/connectors/csv_connectors/):

# +
import pathway as pw


class InputSchema(pw.Schema):
    key: int = pw.column_definition(primary_key=True)
    recipient: str
    sender: str


# _MD_SHOW_table = pw.io.jsonlines.read("./input_file.jsonl", schema=InputSchema)
# _MD_SHOW_pw.io.csv.write(table, "./output.csv")
# _MD_SHOW_pw.run()
# -

# With the `input_file` file containing the correct example defined in the introduction, the output is:
# ```
# key,recipient,sender,time,diff
# 1,"Bill H.","Nancy R.",1707985402732,1
# 2,"Harry P.","Hermione G.",1707985402732,1
# 3,"Julian S.","Dick F.",1707985402732,1
# ```
#
# Adding this line `{"key": 4, "recipient": "Juliet", "sender": "Romeo"}` will update the output:
# ```
# key,recipient,sender,time,diff
# 1,"Bill H.","Nancy R.",1707985402732,1
# 2,"Harry P.","Hermione G.",1707985402732,1
# 3,"Julian S.","Dick F.",1707985402732,1
# 4,"Juliet","Romeo",1707985410232,1
# ```
# The removal of the line is also passed to the output:
# ```
# key,recipient,sender,time,diff
# 1,"Bill H.","Nancy R.",1707985402732,1
# 2,"Harry P.","Hermione G.",1707985402732,1
# 3,"Julian S.","Dick F.",1707985402732,1
# 4,"Juliet","Romeo",1707985410232,1
# 4,"Juliet","Romeo",1707985423732,-1
# ```
# The `diff` value is set to `-1`, representing a removal.
#
#
# ### Static case
#
# The JSON Lines connector also supports the static mode: it will read all the data at once and then closes the connection.
# To activate it, you must set the `mode` parameter to `"static"`:

table = pw.io.jsonlines.read("./input_file.jsonl", schema=InputSchema, mode="static")
pw.debug.compute_and_print(table)

# ## Output connector
#
# To output a table in the JSON Lines format, you should use [`pw.io.jsonlines.write`](/developers/api-docs/pathway-io/jsonlines#pathway.io.jsonlines.write).
# It takes two parameters:
# - the table to output
# - the filename.
#
# ```python
# pw.io.jsonlines.write(table, "output_file.jsonl")
# ```
#
# ⚠️ The JSON Lines output connector only works in streaming mode.
#
# ## Complete example
#
# ```python
# import pathway as pw
#
#
# class InputSchema(pw.Schema):
#     key: int = pw.column_definition(primary_key=True)
#     recipient: str
#     sender: str
#
#
# table = pw.io.jsonlines.read("./input_file.jsonl", schema=InputSchema)
# pw.io.jsonlines.write(table, "./output_file.jsonl")
# pw.run()
# ```
#
# By doing the same operations as before (adding the line and removing it), you obtain the following results:
# ```
# {"key":1,"recipient":"Bill H.","sender":"Nancy R.","diff":1,"time":1707987230734}
# {"key":2,"recipient":"Harry P.","sender":"Hermione G.","diff":1,"time":1707987230734}
# {"key":3,"recipient":"Julian S.","sender":"Dick F.","diff":1,"time":1707987230734}
# {"key":4,"recipient":"Juliet","sender":"Romeo","diff":1,"time":1707987260732}
# {"key":4,"recipient":"Juliet","sender":"Romeo","diff":-1,"time":1707987269732}
# ```
#
# This example simply copies the streams of updates.
# To do something more complicated, you can check [our tutorial about the basic table operations](/developers/user-guide/data-transformation/table-operations/) Pathway supports.
