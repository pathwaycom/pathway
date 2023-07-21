use std::collections::HashMap;

use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

pub trait Queue {
    fn queue(&self) -> Self;
}

impl<G: Scope, D: Data> Queue for Stream<G, D> {
    fn queue(&self) -> Stream<G, D> {
        let mut elements = HashMap::new();
        self.unary_notify(Pipeline, "Queue", vec![], move |input, output, notificator| {
            while let Some((time, data)) = input.next() {
                let set = elements.entry(*time).or_insert_with(Vec::new);
                for datum in data.drain(..) { set.push(datum); }
                notificator.notify_at(time);
            }

            for (time, _count) in notificator {
                if let Some(mut data) = elements.remove(&time) {
                    output.session(&time).give_iterator(data.drain(..));
                }
            }
        })
    }
}
