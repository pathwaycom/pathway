# Input Collections

Differential dataflow computations all start from user-controlled input "collections".

For now, let's think of an input collection as a multiset (or a "bag"): a collection of typed elements which may contain duplicates. It can be a bit more complicated than this, for neat and interesting reasons, but life is easier for now if you think of it as a multiset.

## An example

Let's take our skeleton from the previous subsection and add an input collection.

```rust,no_run
    extern crate timely;
    extern crate differential_dataflow;

    use differential_dataflow::input::Input;

    fn main() {

        // define a new computational scope, in which to run BFS
        timely::execute_from_args(std::env::args(), |worker| {

            // create a counting differential dataflow.
            let mut input = worker.dataflow::<usize,_,_>(|scope| {
                // create inputs, build dataflow, return stuff.
                let (input, words) = scope.new_collection();
                words.inspect(|x| println!("seen: {:?}", x));
                input
            });

            // drive the input around here.

        }).unwrap();
    }
```

Here we've created a new input collection in `scope`, which returns a pair `(input, words)`. The first of these, `input`, is a handle that our program can use to change the collection; this is how we insert and remove records. The second, `words`, is a differential dataflow collection on which we can base further differential dataflow computation. These two are linked so that changes our program makes to `input` result in changes to `words` which then result in changes to derived collections.

This isn't a wildly interesting program yet, because we haven't actually changed `input`. Let's do that now, where the code currently says

        // drive the input around here.

Differential dataflow inputs are similar to timely dataflow inputs, if you are familiar with those, but with a few important tweaks. Each input has a "time" it is currently set to. You can `insert(item)` and `remove(item)` to your hearts content, and these changes will take effect at the time currently associated with the input.

For example, we could write:

```rust,no_run
        // drive the input around here.
        input.insert("hello".to_string());
        input.insert("world".to_string());
        input.advance_to(1);
        input.remove("hello".to_string());
        input.insert("goodbye".to_string());
        input.advance_to(2);
```

At this point we have described some insertions at time `0` (the initial time for each input) and some changes at time `1`.

## General updates

While the `insert` and `remove` methods are generally useful, there is a more general method `input.update(data, diff)` which allows you to specify a difference directly. This can be useful if your data are natively described as differences, if your differences are large in magnitude, or in exotic cases where you are not using multisets (?!?).

## Advancing time

The calls to `input.advance_to()` are surprisingly important. Not only do they indicate that you would like your next changes to happen at a particular new time, these are the calls that signal to the system that you have finished changing the collection at times prior to your new time. It is not until you advance the time of your inputs that the system can determine the correct answer, because until then you could show up with arbitrary changes.

It is crucial to call `input.advance_to(time)` if you want to see the output changes for `time`.

## Flushing

The calls to `insert()`, `remove()`, and `advance_to()` are buffered in the interest of efficiency, and you may need to call `input.flush()` to ensure that every change you've applied to the input is visible to the system. This is unlike timely dataflow, which does not buffer its `advance_to` calls.