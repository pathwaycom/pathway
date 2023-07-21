## Step 1: Write a program.

You write differential dataflow programs against apparently static input collections, with operations that look a bit like database (SQL) or big data (MapReduce) idioms. This is actually a bit of a trick, because you will have the ability to change the input data, but we'll pretend we don't know that yet.

Let's write a program with one input: a collection `manages` of pairs `(manager, person)` describing people and their direct reports. Our program will determine for each person their manager's manager (where the boss manages the boss's own self). If you are familiar with SQL, this is an "equijoin", and we will write exactly that in differential dataflow.

If you are following along at home, put this in your `src/main.rs` file.

```rust,no_run
    extern crate timely;
    extern crate differential_dataflow;

    use differential_dataflow::input::InputSession;
    use differential_dataflow::operators::Join;

    fn main() {

        // define a new timely dataflow computation.
        timely::execute_from_args(std::env::args(), move |worker| {

            // create an input collection of data.
            let mut input = InputSession::new();

            // define a new computation.
            worker.dataflow(|scope| {

                // create a new collection from our input.
                let manages = input.to_collection(scope);

                // if (m2, m1) and (m1, p), then output (m1, (m2, p))
                manages
                    .map(|(m2, m1)| (m1, m2))
                    .join(&manages)
                    .inspect(|x| println!("{:?}", x));
            });

            // Read a size for our organization from the arguments.
            let size = std::env::args().nth(1).unwrap().parse().unwrap();

            // Load input (a binary tree).
            input.advance_to(0);
            for person in 0 .. size {
                input.insert((person/2, person));
            }

        }).expect("Computation terminated abnormally");
    }
```

This program has a bit of boilerplate, but at its heart it defines a new input `manages` and then joins it with itself, once the fields have been re-ordered. The intent is as stated in the comment:

```rust,no_run
    // if (m2, m1) and (m1, p), then output (m1, (m2, p))
```

We want to report each pair `(m2, p)`, and we happen to also produce as evidence the `m1` connecting them.

When we execute this program we get to see the skip-level reports for the small binary tree we loaded as input:

        Echidnatron% cargo run -- 10
             Running `target/debug/my_project`
            ((0, (0, 0)), 0, 1)
            ((0, (0, 1)), 0, 1)
            ((1, (0, 2)), 0, 1)
            ((1, (0, 3)), 0, 1)
            ((2, (1, 4)), 0, 1)
            ((2, (1, 5)), 0, 1)
            ((3, (1, 6)), 0, 1)
            ((3, (1, 7)), 0, 1)
            ((4, (2, 8)), 0, 1)
            ((4, (2, 9)), 0, 1)
        Echidnatron%

This is a bit crazy, but what we are seeing is many triples of the form

        (data, time, diff)

describing how the data have *changed*. That's right; our input is actually a *change* from the initially empty input. The output is showing us that at time `(Root, 0)` several tuples have had their frequency incremented by one. That is a fancy way of saying they are the output.

This may make more sense in just a moment, when we want to *change* the input.