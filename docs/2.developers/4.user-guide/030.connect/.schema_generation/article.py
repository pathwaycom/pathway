# ---
# title: 'Automatic Generation of Schema Class'
# description: 'Tutorial on generating Schema from sample CSV file or definition in JSON'
# date: '2023-11-08'
# thumbnail: '/assets/content/blog/th-custom-connectors.png'
# tags: ['tutorial', 'table']
# keywords: ['schema', 'type', 'JSON', 'connectors']
# notebook_export_path: notebooks/tutorials/schema-generation.ipynb
# ---

# # Automatic Generation of Schema in Pathway
#
# In this article, you will learn how to easily generate Schemas, which are used to define the
# structure of a Pathway Table.


# In Pathway, Schemas are used to define the structure of a Table, that is, names and types
# of columns. To learn the basics of Schemas in Pathway, and how you can create them by writing
# a class definition in Python, read our [user guide](/developers/user-guide/connect/schema/).
# In this tutorial, you will learn alternative ways to create Schemas, either by providing a dictionary of column definitions or sample input data in CSV format.

# To run the examples in this tutorial, you can download the needed files by uncommenting and executing the following code:

# %%capture --no-display
# # !wget https://public-pathway-releases.s3.eu-central-1.amazonaws.com/data/schema-generation-sample-data.csv -O data.csv
# # !wget https://public-pathway-releases.s3.eu-central-1.amazonaws.com/data/schema-generation-schema.json -O schema.json


# ## Schema generation from JSON
#
# In Pathway, you can build a Schema from a dictionary using the
# [schema_builder function](/developers/user-guide/connect/schema#schema-as-a-dictionary), described in our user guide.
# The values in the dictionary given as an input to `pw.schema_builder`, however, are instances of `pw.column_definition`, which has some limitations, e.g. they can't be easily serialized in a JSON file. For this purpose, in Pathway there is a `pw.schema_from_dict` function, which takes
# as an input a dictionary whose keys are names of the columns, and values are either:
# - type of a column
# - a dictionary with keys "dtype", "primary_key", "default_value", which define respectively the type of the column, whether it is a primary key of the Table, and what is its default value.

# To see a working example of `pw.schema_from_dict`, start by creating a JSON file with a definition of the Schema you want to generate.

# ```JSON
# {
#     "key": {
#         "dtype": "int",
#         "primary_key": true
#     },
#     "name": {
#         "dtype": "str",
#         "default_value": ""
#     },
#     "value": "int"
# }
# ```


# Now, you need to load it into Python, where it will be represented as a `dict` type.

# +
import json

with open("schema.json") as f:
    schema_definition = json.load(f)
# -

# Then all you need to do is pass it as an argument to `pw.schema_from_dict` which will return a Schema object.

# +
import pathway as pw

schema = pw.schema_from_dict(schema_definition)

# Check the resulting Schema
schema, schema.primary_key_columns(), schema.default_values()
# -

# You can now use the created Schema as an argument to the connector you want to use.

# ## Schema generation from data in a CSV file
#
# If you have a CSV file with sample data, you can use it to generate a schema in Pathway. The name of columns will be taken from the header of the CSV file, whereas types of columns are inferred by checking if values in the given column can be parsed to int or float. This method of Schema generation does not support choosing primary keys or setting default values. Still, once you generate a Schema, you can generate a class definition using the method described later in this tutorial and then edit it to suit your needs.
#
# To generate a Schema based on a CSV file, use a `pw.schema_from_csv` function. To see how it works, you will use the following example data in CSV format:
# ```csv
# age,owner,pet
# 10,Alice,dog
# 9,Bob,dog
# 8,Alice,cat
# 7,Bob,dog
# ```

# To use `pw.schema_from_csv`, you only need to provide a path of CSV file, but it also has some number of optional arguments:
# - name - the name of the resulting Schema class
# - properties - an instance of `SchemaProperties`
# - delimiter - the delimiter used in the CSV file
# - comment_character - the character used to denote that a row is a comment
# - escape - the escape character used in the CSV file
# - quote - the character used to quote fields
# - enable_double_quote_escapes - enables escaping quotes by using double quotes
# - num_parsed_rows - how many rows should be parsed. If None, all rows will be parsed.

# For the example, you can use the default values of the optional arguments, so the Python code is:

# +
schema = pw.schema_from_csv("data.csv")

# Check the resulting Schema
schema
# -

# ## Persisting generated Schema as a Python class definition
#
# If you have a Schema object, you may choose to generate a class definition, either
# to make the codebase independent of other files you need to generate the Schema or to
# change it, for example, adding default values to a Schema generated based on a CSV file.
#
# To do that, you can use a method in the Schema class called `generate_class` to generate a string with a class definition or `generate_class_to_file` to generate a class definition and save it to a file.

# Let's go through an example of using `generate_class` and `generate_class_to_file`. In the example, you will work on the schema generated in the Section on `schema_from_dict`.

with open("schema.json") as f:
    schema_definition = json.load(f)
schema = pw.schema_from_dict(schema_definition)

# The first method - `generate_class` - has no required arguments, and two optional argument - `class_name`, is the name of the class with the generated Schema and `generate_imports` specifies if imports of modules used in the Class definition should be included in the beginning of the string. If `class_name` is not provided, the schema's name will be used, or if it is not a correct identifier, the default name `CustomSchema` will be used.

print(schema.generate_class(class_name="MySchema"))

# Method `generate_class_to_file` has one required argument, which is the path where class definition is to be saved. Its optional arguments `class_name` and `generate_includes` are the same as for `generate_class` method.

schema.generate_class_to_file(
    "myschema.py", class_name="MySchema", generate_imports=True
)
with open("myschema.py") as f:
    print(f.read())
