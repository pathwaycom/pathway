# Trace wrappers

There are many cases where we make small manipulations of a collection, and we might hope to retain the arrangement structure rather than re-build and maintain new arrangements. In some cases this is possible, using what we call *trace wrappers*.

The set of trace wrappers grows as more idioms are discovered and implemented, but the intent is that we can often avoid reforming new collections, and instead just push logic into a layer around the arrangement.

## Filter

Like a collection, an arrangement supports the `filter(predicate)` operator that reduces the data down to those elements satisfying `predicate`. Unlike a collection, which produces a new collection when filtered, a filtered arrangement is just a wrapper around the existing arrangement.

The following example uses two different collections in its two joins, but one is a filtered version of the other and can re-use the same arrangement.

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
            let knows1 = knows.arrange_by_key();

            // Filter to equal pairs (for some reason).
            let knows2 = knows1.filter(|k,v| k == v);

            // Same logic as before, with a new method name.
            query.join_core(&knows1, |x,q,y| Some((*y,(*x,*q))))
                 .join_core(&knows2, |y,(x,q),z| Some((*q,(*x,*y,*z))))
                 .inspect(|result| println!("result {:?}", result));

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

Filtered arrangements are not always a win. If the input arrangement is large and the filtered arrangement is small, it may make more sense to build and maintain a second arrangement than to continually search through the large arrangement for records satisfying the predicate. If you would like to form a second arrangement, you can use `as_collection()` to return to a collection, filter the result, and then arrange it again.

## Entering scopes

Differential dataflow programs often contain nested scopes, used for loops and iteration. Collections in a nested scope have different timestamps than collections outside the scope, which means we can not immediately re-use arrangements from outside the scope inside the scope.

Like collections, arrangements support an `enter(scope)` method for entering a scope, which will wrap the arrangement so that access to timestamps automatically enriches it as if the collection had entered the scope.

The following example demonstrates arranging the `knows` relation outside an iterative scope, and then bringing it in to the scope (along with the collection `query`). Unlike `query`, which is a collection, `knows` is an arrangement and will simply be wrapped with timestamp-extending logic.

```rust
extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::operators::Join;
use differential_dataflow::operators::Threshold;
use differential_dataflow::operators::Iterate;
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

            // Reachability queries.
            query.iterate(|reach| {

                let knows = knows.enter(&reach.scope());
                let query = query.enter(&reach.scope());

                knows.join_map(reach, |x,y,q| (*y,*q))
                     .concat(&query)
                     .distinct()
            });

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

## Other wrappers

Other wrappers exist, but are still in development and testing. Generally, if the same physical layout of the index would support a collection transformation, a wrapper may be appropriate. If you think you have such an operation, the `src/trace/wrappers/` directory is where the current examples reside.