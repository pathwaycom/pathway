extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

extern crate dogsdogsdogs;

use timely::dataflow::Scope;
use timely::order::Product;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::UnorderedInput;
use timely::dataflow::operators::Map;
use differential_dataflow::AsCollection;

fn main() {

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let mut probe = Handle::new();

        let (mut i1, mut i2, c1, c2) = worker.dataflow::<usize,_,_>(|scope| {

            // Nested scope as `Product<usize, usize>` doesn't refine `()`, because .. coherence.
            scope.scoped("InnerScope", |inner| {

                use timely::dataflow::operators::unordered_input::UnorderedHandle;

                let ((input1, capability1), data1): ((UnorderedHandle<Product<usize, usize>, ((usize, usize), Product<usize, usize>, isize)>, _), _) = inner.new_unordered_input();
                let ((input2, capability2), data2): ((UnorderedHandle<Product<usize, usize>, ((usize, usize), Product<usize, usize>, isize)>, _), _) = inner.new_unordered_input();

                let edges1 = data1.as_collection();
                let edges2 = data2.as_collection();

                // Graph oriented both ways, indexed by key.
                use differential_dataflow::operators::arrange::ArrangeByKey;
                let forward1 = edges1.arrange_by_key();
                let forward2 = edges2.arrange_by_key();

                // Grab the stream of changes. Stash the initial time as payload.
                let changes1 = edges1.inner.map(|((k,v),t,r)| ((k,v,t.clone()),t,r)).as_collection();
                let changes2 = edges2.inner.map(|((k,v),t,r)| ((k,v,t.clone()),t,r)).as_collection();

                use dogsdogsdogs::operators::half_join;

                // pick a frontier that will not mislead TOTAL ORDER comparisons.
                let closure = |time: &Product<usize, usize>| Product::new(time.outer.saturating_sub(1), time.inner.saturating_sub(1));

                let path1 =
                half_join(
                    &changes1,
                    forward2,
                    closure.clone(),
                    |t1,t2| t1.lt(t2),  // This one ignores concurrent updates.
                    |key, val1, val2| (key.clone(), (val1.clone(), val2.clone())),
                );

                let path2 =
                half_join(
                    &changes2,
                    forward1,
                    closure.clone(),
                    |t1,t2| t1.le(t2),  // This one can "see" concurrent updates.
                    |key, val1, val2| (key.clone(), (val2.clone(), val1.clone())),
                );

                // Delay updates until the worked payload time.
                // This should be at least the ignored update time.
                path1.concat(&path2)
                    .inner.map(|(((k,v),t),_,r)| ((k,v),t,r)).as_collection()
                    .inspect(|x| println!("{:?}", x))
                    .probe_with(&mut probe);

                (input1, input2, capability1, capability2)
            })
        });

        i1
            .session(c1.clone())
            .give(((5, 6), Product::new(0, 13), 1));

        i2
            .session(c2.clone())
            .give(((5, 7), Product::new(11, 0), 1));

    }).unwrap();
}
