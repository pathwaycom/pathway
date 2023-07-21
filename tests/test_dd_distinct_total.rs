use std::sync::{Arc, Mutex};

use differential_dataflow::input::Input;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::Data;
use eyre::{eyre, Result};
use timely::dataflow::operators::Inspect;
use timely::dataflow::{Scope, Stream};

use pathway_engine::engine::dataflow::operators::ArrangeWithTypes;

fn collect_stream<S, D>(stream: &Stream<S, D>) -> Arc<Mutex<Vec<D>>>
where
    S: Scope,
    D: Data,
{
    let res = Arc::new(Mutex::new(Vec::new()));
    stream.inspect({
        let res = res.clone();
        move |data| {
            eprintln!("{data:?}");
            res.lock().unwrap().push(data.clone());
        }
    });
    res
}

#[test]
fn test_add_remove_add() -> Result<()> {
    let distinct_total = timely::execute_directly(move |worker| -> Result<_> {
        let (mut input_session, distinct_total) = worker.dataflow(|scope| -> Result<_> {
            let (input_session, input) = scope.new_collection();

            input.inspect(|(data, time, diff)| {
                eprintln!("input: @{time:?} {diff:+} {data:?}");
            });

            let arranged = input.arrange::<OrdKeySpine<i32, _, _>>();

            let distinct_total = arranged.distinct_total();

            Ok((input_session, collect_stream(&distinct_total.inner)))
        })?;

        input_session.update_at(42, 0, 1);
        input_session.update_at(42, 1, -1);
        input_session.update_at(42, 2, 1);
        input_session.close();

        Ok(distinct_total)
    })
    .map_err(|e| eyre!("timely error: {e}"))?;

    let distinct_total = Arc::try_unwrap(distinct_total)
        .unwrap()
        .into_inner()
        .unwrap();

    assert_eq!(distinct_total, vec![(42, 0, 1), (42, 1, -1), (42, 2, 1)]);

    Ok(())
}
