# ---
# title: File System Connector
# description: An article explaining how to read from files and write to files in a few basic data formats.
# date: '2024-01-26'
# tags: ['tutorial', 'table']
# tech_icons: ["lets-icons:file-fill"]
# deployment_tag: ["jupyter", "docker"]
# keywords: ['connector', 'file system', 'csv', 'json', 'input', 'output', 'static', 'streaming']
# notebook_export_path: notebooks/tutorials/fs_connector.ipynb
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # File System Connectors
# This guide explains the [fs connectors](/developers/api-docs/pathway-io/fs) that connect Pathway to your file system to read and write data with the following basic formats: binary, plaintext, CSV, and JSON.
#
# The first part of this guide focuses on defining the source of the data for our connector (using plaintext data format to keep things simple). The second part explains additional configuration that can (or needs to) be defined for all simple data formats.
# In particular we show the input connectors ([`pw.io.fs.read`](/developers/api-docs/pathway-io/fs#pathway.io.fs.read)) reading data in:
# - [`CSV` format](/developers/user-guide/connect/connectors/fs-connector#csv),
# - [`JSON` format](/developers/user-guide/connect/connectors/fs-connector#json),
# - [`plaintext`, `plaintext_by_file`, and `binary` formats](/developers/user-guide/connect/connectors/fs-connector#unstructured-data).
#
# The output connectors ([`pw.io.fs.write`](/developers/api-docs/pathway-io/fs#pathway.io.fs.write)) write data in:
# - [`CSV` format](/developers/user-guide/connect/connectors/fs-connector#csv),
# - [`JSON` format](/developers/user-guide/connect/connectors/fs-connector#json).
#
# File system connectors work both in streaming and static modes. Be careful as the use of connectors differs depending on the chosen mode: see the [differences](/developers/user-guide/introduction/streaming-and-static-modes).
# For simplicity, all the examples below are in the "static" mode but can easily be changed to "streaming" mode by changing the `mode` parameter.
#
# ## Location of files and filter.
# The code snippets below prepares the basic file structure that is used in the later part of this article. To keep tings simple, all examples work with data of type `str`, to see more on [schemas](/developers/user-guide/connect/schema) and [types](/developers/user-guide/connect/datatypes) in other places.

# %%
! mkdir -p plain_input
! mkdir -p plain_output
! echo -e "test1\ndata1" > plain_input/in1.txt
! echo -e "test2\ndata2" > plain_input/in2.txt
# %% [markdown]
# ### Specify the input and output with `path` and `filename`.
# Below, you can find the simplest examples of input ([`pw.io.fs.read`](/developers/api-docs/pathway-io/fs#pathway.io.fs.read)) and output ([`pw.io.fs.write`](/developers/api-docs/pathway-io/fs#pathway.io.fs.write)) connectors. Both examples use plaintext as the input format (more on that [later](/developers/user-guide/connect/connectors/fs-connector#data-formats)). The `path` parameter can point either to a directory or a particular file. If it point so a directory, it reads all files that are inside. Otherwise it reads only the file that is specified (and as such it makes sense only in the static mode).

# %%
%%capture
import pathway as pw
test1 = pw.io.fs.read(path = "./plain_input/", format = "plaintext", mode="static")
pw.io.fs.write(test1, filename="./plain_output/out1.txt", format="json")

test2 = pw.io.fs.read(path = "./plain_input/in1.txt", format = "plaintext", mode="static")
pw.io.fs.write(test2, filename="./plain_output/out2.txt", format="json")

pw.run()

# %% [markdown]
# The output can be found in the `plain_output` directory.

# %%
! echo "out1:"
! cat plain_output/out1.txt
! echo "out2:"
! cat plain_output/out2.txt

# %% [markdown]
# As you can see, the first example read the data from both `in1.txt` and `in2.txt`, while the second read only the data from `in1.txt`.

# ### Filter the files to read with `object_pattern`
# In case you want to specify a directory as the source of your data, but read only some of its contents, you can specify filter [pattern](https://www.gnu.org/software/findutils/manual/html_node/find_html/Shell-Pattern-Matching.html) and pass it using the `object_pattern` parameter.
# %%
%%capture
test3 = pw.io.fs.read("./plain_input/", format = "plaintext", mode="static", object_pattern = "*2*")
pw.io.fs.write(test3, "./plain_output/output3.txt", "json")
pw.run()

# %% [markdown]
# The output can be found in the `plain_output` directory. As you can see, `out3.txt` contains data only from `in2.txt`, as it is the only file in the input directory that matches the `*2*` pattern:

# %%
! echo "out3:"
! cat plain_output/output3.txt

# %% [markdown]
# ## Data formats
# ### CSV
# For the CSV format, each file on the input needs to have defined headers.

# %%
! mkdir -p csv_input
! mkdir -p csv_output
! echo -e "header1;header2\ndata1;data2\n\ndata3;data4" > csv_input/csv_in1.txt
! echo -e "header1;header2\ndata5;data6\n\ndata7;data8" > csv_input/csv_in2.txt
! echo -e "csv_in1.txt:"
! cat csv_input/csv_in1.txt
! echo -e "csv_in2.txt:"
! cat csv_input/csv_in2.txt
# %% [markdown]
# In most cases, in order to read the data, you need to define its schema and pass it to the connector. Furthermore, for the `csv` format, you can use [CSVParserSettings](/developers/api-docs/pathway-io#pathway.io.CsvParserSettings) to accommodate for a nonstandard formatting of the input file. In the example below, it is configured to use `;` as a delimiter.
# %%
%%capture
class csv_schema(pw.Schema):
    header1: str
    header2: str

csv_settings = pw.io.CsvParserSettings(delimiter=";")
csv_data = pw.io.fs.read(path = "./csv_input/", format="csv", schema=csv_schema, csv_settings=csv_settings,mode="static")
pw.io.fs.write(table=csv_data, filename="./csv_output/csv_out1.txt", format="csv")
pw.run()
# %%
! cat ./csv_output/csv_out1.txt

# %% [markdown]
# You can also use the dedicated [CSV connector](/developers/user-guide/connect/connectors/csv_connectors).
# ### JSON
# You can use the [JSON format](https://json.org) by setting the parameter `format` to `json`.
# %%
! mkdir -p json_input
! mkdir -p json_output
! echo -e '{"header1":data1",\n"header2":"data2"}\n{"header1":"data3","header2":"data4"}\n' > json_input/json_in1.txt
! echo -e '{"header1":"data5","header2":"data6"}\n{"header1":"data7","header2":"data8"}\n' > json_input/json_in2.txt
! echo -e "json_in1.txt:"
! cat json_input/json_in1.txt
! echo -e "json_in2.txt:"
! cat json_input/json_in2.txt
# %% [markdown]
# As in most cases, in order to read the data, you need to define a schema and pass it to the connector. Each input file needs to be a sequence of properly formatted JSON objects.
# %%
%%capture
class json_schema(pw.Schema):
    header1: str
    header2: str

json_data = pw.io.fs.read(path = "./json_input/", format="json", schema=json_schema, mode="static")
pw.io.fs.write(table=json_data, filename="./json_output/json_out1.txt", format="json")
pw.run()
# %%
! cat ./json_output/json_out1.txt
# %% [markdown]
# ### Unstructured data
# Pathway allows you to read unstructured data using three formats: `plaintext`, `plaintext_by_file`, and  `binary`. `binary` and `plaintext` considers each line as a separate row that will be stored in the column `data`, and the format `plaintext_by_file` treats each file as a single row.
# %%
! mkdir -p unstructured_output

# %%
%%capture
plaintext_data = pw.io.fs.read(path = "./plain_input", format = "plaintext", mode="static")
pw.io.fs.write(plaintext_data,"./unstructured_output/output1.txt", "csv")

plaintext_by_file_data = pw.io.fs.read(path = "./plain_input", format = "plaintext_by_file", mode="static")
pw.io.fs.write(plaintext_by_file_data,"./unstructured_output/output2.txt", "csv")

binary_data = pw.io.fs.read(path = "./plain_input", format = "binary", mode="static")
pw.io.fs.write(binary_data,"./unstructured_output/output3.txt", "csv")

pw.run()

# %%
! echo "plaintext"
! cat ./unstructured_output/output1.txt
! echo "plaintext by file"
! cat ./unstructured_output/output2.txt
! echo "binary"
! cat ./unstructured_output/output3.txt


