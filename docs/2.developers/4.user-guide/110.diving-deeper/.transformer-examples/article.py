# ---
# title: "Examples of transformer classes"
# description: 'Several examples of Pathway transformer classes'
# notebook_export_path: notebooks/tutorials/transformers.ipynb
# ---

# # A tour of Pathway's transformer classes
#
# In this section, we will go through several examples of Pathway transformer classes.
# This should give you a good overview of how to handle them and how useful they are.
#
# We will not go into implementation details, so you are strongly encouraged to read [our introduction](/developers/user-guide/diving-deeper/transformer-introduction/) first.
#
# In the following, we are going to see how to use transformer classes to perform [simple operations on a single row](#simple-operations-on-a-single-row), use [transformers as a method](#transformers-as-a-method), and use transformers to [combine several tables at once](#transformer-classes-using-two-different-tables).
#
# ## Our guinea pig
#
# You will experiment on the following table:

# +
from typing import Any

import pathway as pw

guinea_pig = pw.debug.table_from_markdown(
    """
    | val  | aux
 0  | 0    | 10
 1  | 1    | 11
 2  | 2    | 12
 3  | 3    | 13
 4  | 4    | 14
 5  | 5    | 15
 6  | 6    | 16
 """
)


# -

# ## Simple operations on a single row
#
# First, you are going to perform simple operations on the table: adding a given number, obtaining the squared value, and performing the sum of two columns.
#
# ### Adding 10 to each value:

# +
@pw.transformer
class add_ten:
    class table(pw.ClassArg):
        val = pw.input_attribute()

        @pw.output_attribute
        def result(self) -> float:
            return self.val + 10


result = add_ten(guinea_pig).table
pw.debug.compute_and_print(result)


# -

# As you can see only the column `val` has been taken into account.
#
# ### Obtaining the squared value of each value:

# +
@pw.transformer
class squared_value:
    class table(pw.ClassArg):
        val = pw.input_attribute()

        @pw.output_attribute
        def result(self) -> float:
            return self.val * self.val


result = squared_value(guinea_pig).table
pw.debug.compute_and_print(result)


# -

# ### Summing two columns

# +
@pw.transformer
class summing_columns:
    class table(pw.ClassArg):
        val = pw.input_attribute()
        aux = pw.input_attribute()

        @pw.output_attribute
        def result(self) -> float:
            return self.val + self.aux


result = summing_columns(guinea_pig).table
pw.debug.compute_and_print(result)


# -

# Those three results can be obtained by a unique transformer:

# +
@pw.transformer
class combined_transformer:
    class table(pw.ClassArg):
        val = pw.input_attribute()
        aux = pw.input_attribute()

        @pw.output_attribute
        def result_add(self) -> float:
            return self.val + 10

        @pw.output_attribute
        def result_squared(self) -> float:
            return self.val * self.val

        @pw.output_attribute
        def result_sum(self) -> float:
            return self.val + self.aux


result = combined_transformer(guinea_pig).table
pw.debug.compute_and_print(result)


# -

# Finally, you can use the new values inside the same transformer to perform more advanced operations:

# +
@pw.transformer
class reusing_transformer:
    class table(pw.ClassArg):
        val = pw.input_attribute()

        @pw.output_attribute
        def result_add(self) -> float:
            return self.val + 10

        @pw.output_attribute
        def result_double(self) -> float:
            return self.result_add + self.result_add


result = reusing_transformer(guinea_pig).table
pw.debug.compute_and_print(result)


# -

# ## Transformers as a method
#
# You are not bound to static computation as transformers provide a way to obtain methods as new values.
# This is done using the `method` keyword:

# +
@pw.transformer
class method_transformer:
    class table(pw.ClassArg):
        val: float = pw.input_attribute()

        @pw.method
        def method_result(self, arg) -> float:
            return self.val + arg


method_table = method_transformer(guinea_pig).table
result = method_table.select(res=method_table.method_result(10))
pw.debug.compute_and_print(result)
# -

# ## Transformer Classes using two different tables
#
# Now you might want to do something more complicated which requires two different tables.
#
# You have a table `matchings` which contains pairs of values `a` and `b` and a table `profiles` which contains the profile of each value of the pairs.
# You want to compute, for each pair, the sum of the profiles of the values of the pair.
#
# First, you need the tables:


# +
profiles = pw.debug.table_from_markdown(
    """
    | profile
 0  | 1
 1  | 10
 2  | 100
 3  | 1000
 """
)

matchings = pw.debug.table_from_markdown(
    """
    | a  | b
 0  | 0  | 2
 1  | 1  | 3
 """
)
matchings = matchings.select(
    a=profiles.pointer_from(matchings.a), b=profiles.pointer_from(matchings.b)
)


# -

# Now, you can do a transformer which takes the two tables as parameters.
# To access a given table inside the transformer, use the notation `self.transformer.my_table`.

# +
@pw.transformer
class using_two_tables:
    class profiles_table(pw.ClassArg):
        profile: float = pw.input_attribute()

    class matchings_table(pw.ClassArg):
        a: pw.Pointer = pw.input_attribute()
        b: pw.Pointer = pw.input_attribute()

        @pw.output_attribute
        def sum_profiles(self) -> float:
            pa = self.transformer.profiles_table[self.a].profile
            pb = self.transformer.profiles_table[self.b].profile
            return pa + pb


result = using_two_tables(
    profiles_table=profiles, matchings_table=matchings
).matchings_table
pw.debug.compute_and_print(result)
# -

# ## Other topics
#
# We hope these examples make you feel comfortable using Pathway transformer classes. You can take a look at our advanced example of [transformer classes on a tree](/developers/user-guide/diving-deeper/transformer-recursion/).
#
# To continue your exploration of Pathway, you can also check out our [connectors](/developers/user-guide/connecting-to-data/connectors/), or see directly how to use Pathway to implement classic algorithms such as [PageRank](/developers/showcases/pagerank).
