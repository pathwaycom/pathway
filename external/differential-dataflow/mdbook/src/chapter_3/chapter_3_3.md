## Making Changes

An `InputSession` instance supports several methods allowing us to change the underlying collection.

The simplest of these methods are `insert(item)` and `remove(item)`, which respectively increment and decrement the count of the supplied item, at the current time of the input session.

The `update(item, diff)` method allows you to specify an arbitrary change to an item, positive or negative and of arbitrary magnitude. This method can be useful when your input data come as differences, or if you need to deal with large magnitude changes.

The `update_at(item, time, diff)` method allows you to specify an arbitrary change to an item, at an arbitrary time in the present or future of the current time of the input session. This method can be useful if your data may arrive out of order with respect to time, and you would nonetheless like to introduce the data to the computation. You are not likely to see the effects of these changes until the input passes their associated times, but you may nonetheless want to introduce the data rather than buffer it yourself.

### Timely streams

As indicated in the "Creating Inputs" section, any timely dataflow stream can be recast as a differential dataflow collection, and so any other source of changes that can be turned into a timely dataflow stream can also be used as input changes for a differential dataflow computation. Doing this requires some care, which is what the `InputSession` type tries to provide for you.