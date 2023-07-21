# Using arrangements

The same collection can be arranged multiple different ways. Although the contents of the collection are the same, the different arrangements are useful in different contexts.

## Arrangement by key

We saw before an example where we used one type of arrangement, `arrange_by_key()`, to re-use the same arrangement for multiple `join` operators. The "by key" arrangement is great for operators that require `(key, val)` input data grouped by `key`, and join is one operator that does require that.

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
            query.join_core(&knows, |x,q,y| Some((*y,(*x,*q))))
                 .join_core(&knows, |y,(x,q),z| Some((*q,(*x,*y,*z))))
                 .inspect(|result| println!("result {:?}", result));

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

## Arrangement by self

Another form of arrangement is "by self", where the elements of the collection themselves are taken as keys with no associated values. Arrangement by self is important for certain operators like `distinct`, `count`, and `semijoin`, each of which just need access to indexed records but without associated values.

We can show off arrangement by self in our "friends of friends" example by adding some requirements to our `(x, y, z)` output triples. Let's imagine that in addition, we want each of the other four "knows" relationships, in addition to the two we start with ("x knows y" and "y knows z").

```rust
extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(::std::env::args(), move |worker| {

        let mut knows = differential_dataflow::input::InputSession::new();
        let mut query = differential_dataflow::input::InputSession::new();

        worker.dataflow(|scope| {

            let knows = knows.to_collection(scope);
            let query = query.to_collection(scope);

            // Arrange the data first! (by key and self).
            let knows_by_key = knows.arrange_by_key();
            let knows_by_self = knows.arrange_by_self();

            // The same outputs as in the previous example.
            let candidates =
            query.join_core(&knows_by_key, |x,q,y| Some((*y,(*x,*q))))
                 .join_core(&knows_by_key, |y,(x,q),z| Some((*q,(*x,*y,*z))));

            // Repeatedly put pairs of nodes as keys, and semijoin with knows.
            candidates
                .map(|(q,(x,y,z))| ((x,z),(q,y)))
                .join_core(&knows_by_self, |&(x,z),&(q,y),&()| Some(((y,z),(q,x))))
                .join_core(&knows_by_self, |&(y,z),&(q,x),&()| Some(((z,x),(q,y))))
                .join_core(&knows_by_self, |&(z,x),&(q,y),&()| Some(((y,x),(q,z))))
                .join_core(&knows_by_self, |&(y,x),&(q,z),&()| Some((q,(x,y,z))))
                .inspect(|result| println!("result {:?}", result));

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

We now have two arranged forms of the `knows` collection, which is more than before, but we have now used the collection six times in our computation. We now have a factor three reduction in required resources from the corresponding naive implementation!

## Returning to collections

You may need to return from an arrangement to a collection (a stream of updates). An arrangement's `as_collection()` method allows this.


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

            // Return to collection representation.
            let knows = knows.as_collection(|k,v| (*k,*v));

            // Same logic as before, with a new method name.
            query.join_map(&knows, |x,q,y| (*y,(*x,*q))))
                 .join_map(&knows, |y,(x,q),z| (*q,(*x,*y,*z)))
                 .inspect(|result| println!("result {:?}", result));

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```
