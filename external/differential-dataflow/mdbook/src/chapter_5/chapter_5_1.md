# An arrangement example

Imagine you have collection that describes a relation among people, perhaps "x knows y", and you would like to query the "friends of friends" relation: for a given person x, who are the people known by friends of x?

Let's first build this naively, starting from two inputs: `knows` containing the pairs of the relation and `query` containing pairs `(source, query_id)` that allow us to interactively interrogate the data.

```rust
extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::operators::Join;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(::std::env::args(), move |worker| {

        let mut knows = differential_dataflow::input::InputSession::new();
        let mut query = differential_dataflow::input::InputSession::new();

        worker.dataflow(|scope| {

            let knows = knows.to_collection(scope);
            let query = query.to_collection(scope);

            // Hop from x to y, then from y to z.
            query.join_map(&knows, |x,q,y| (*y,*q))
                 .join_map(&knows, |y,q,z| (*q,*z))
                 .inspect(|result| println!("result {:?}", result));

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

As it happens, differential dataflow's join operations all do the same things with their input collections: they will convert the stream of updates into an indexed representation (which they then use to respond to change in the other inputs). This makes `join` an excellent candidate to use arrangements as inputs.

To arrange a collection, we just call one of several `arrange` methods. In this case, we will arrange "by key", because we want to take our `(x, y)` pairs and arrange them by x. Once we have done this, there are just a few additional cosmetic changes to make to our program to use this arranged data in each join:

```rust
extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::arrange::ArrangeByKey;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(::std::env::args(), move |worker| {

        let mut knows = differential_dataflow::input::InputSession::new();
        let mut query = differential_dataflow::input::InputSession::new();

        worker.dataflow(|scope| {

            let knows = knows.to_collection(scope);
            let query = query.to_collection(scope);

            // Arrange the data first! (by key).
            let knows = knows.arrange_by_key();

            // Same logic as before, with a new method name.
            query.join_core(&knows, |x,q,y| Some((*y,*q)))
                 .join_core(&knows, |y,q,z| Some((*q,*z)))
                 .inspect(|result| println!("result {:?}", result));

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

Our computation now contains only one copy of the potentially large and fast-changing `knows` collection. This not only saves on memory for collection, but it also saves on the computation and communication required to maintain the indexed representation as the collection changes.

