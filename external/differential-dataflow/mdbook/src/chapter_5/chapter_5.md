# Arrangements

Differential dataflow acts on *collections* of data, each of which we think of as a growing set of update triples `(data, time, diff)`. Arrangements are a new way to represent a set of updates, which can be substantially more efficient than our approaches so far (and all other stream processors to date).

Thus far, we have implemented differential collections as streams of update triples, which we connect as inputs to our various differential operators. At any moment an operator might receive a few more update triples and need to react to them. Or an operator might receive the signal that some timestamp `time` is now complete and we should expect no more updates bearing that timestamp.

Streams of updates are a fine representation of collections, but they leave a great amount of performance on the table. In fact, many operators do *exactly the same thing* with their input update streams: they build and maintain an index of the updates so that they can randomly access them in the future. With that in mind, why not build and maintain that index once, sharing the required resources across the operators that use the indexed data instead of asking each to perform redundant work?

Arrangements are an indexed representation of streamed data. An arrangement indexes batches of update tuples and streams these indexed batches in place of individual update tuples. At the same time, it maintains the sequence of these indexed batches in a compact representation, merging batches as appropriate so that all users have access to an efficient index of all updates so far.
