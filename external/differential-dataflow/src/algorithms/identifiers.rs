//! Assign unique identifiers to records.

use timely::dataflow::Scope;

use ::{Collection, ExchangeData, Hashable};
use ::lattice::Lattice;
use ::operators::*;
use ::difference::Abelian;

/// Assign unique identifiers to elements of a collection.
pub trait Identifiers<G: Scope, D: ExchangeData, R: ExchangeData+Abelian> {
    /// Assign unique identifiers to elements of a collection.
    ///
    /// # Example
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::algorithms::identifiers::Identifiers;
    /// use differential_dataflow::operators::Threshold;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let identifiers =
    ///         scope.new_collection_from(1 .. 10).1
    ///              .identifiers()
    ///              // assert no conflicts
    ///              .map(|(data, id)| id)
    ///              .threshold(|_id,cnt| if cnt > &1 { *cnt } else { 0 })
    ///              .assert_empty();
    ///     });
    /// }
    /// ```
    fn identifiers(&self) -> Collection<G, (D, u64), R>;
}

impl<G, D, R> Identifiers<G, D, R> for Collection<G, D, R>
where
    G: Scope,
    G::Timestamp: Lattice,
    D: ExchangeData+::std::hash::Hash,
    R: ExchangeData+Abelian,
{
    fn identifiers(&self) -> Collection<G, (D, u64), R> {

        // The design here is that we iteratively develop a collection
        // of pairs (round, record), where each pair is a proposal that
        // the hash for record should be (round, record).hashed().
        //
        // Iteratively, any colliding pairs establish a winner (the one
        // with the lower round, breaking ties by record), and indicate
        // that the losers should increment their round and try again.
        //
        // Non-obviously, this happens via a `reduce` operator that yields
        // additions and subtractions of losers, rather than reproducing
        // the winners. This is done under the premise that losers are
        // very rare, and maintaining winners in both the input and output
        // of `reduce` is an unneccesary duplication.

        use collection::AsCollection;

        let init = self.map(|record| (0, record));
        timely::dataflow::operators::generic::operator::empty(&init.scope())
            .as_collection()
            .iterate(|diff|
                init.enter(&diff.scope())
                    .concat(&diff)
                    .map(|pair| (pair.hashed(), pair))
                    .reduce(|_hash, input, output| {
                        // keep round-positive records as changes.
                        let ((round, record), count) = &input[0];
                        if *round > 0 {
                            output.push(((0, record.clone()), count.clone().negate()));
                            output.push(((*round, record.clone()), count.clone()));
                        }
                        // if any losers, increment their rounds.
                        for ((round, record), count) in input[1..].iter() {
                            output.push(((0, record.clone()), count.clone().negate()));
                            output.push(((*round+1, record.clone()), count.clone()));
                        }
                    })
                    .map(|(_hash, pair)| pair)
            )
            .concat(&init)
            .map(|pair| { let hash = pair.hashed(); (pair.1, hash) })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn are_unique() {

        // It is hard to test the above method, because we would want
        // to exercise the case with hash collisions. Instead, we test
        // a version with a crippled hash function to see that even if
        // there are collisions, everyone gets a unique identifier.

        use ::input::Input;
        use ::operators::{Threshold, Reduce};
        use ::operators::iterate::Iterate;

        ::timely::example(|scope| {

            let input = scope.new_collection_from(1 .. 4).1;

            use collection::AsCollection;

            let init = input.map(|record| (0, record));
            timely::dataflow::operators::generic::operator::empty(&init.scope())
                .as_collection()
                .iterate(|diff|
                    init.enter(&diff.scope())
                        .concat(&diff)
                        .map(|(round, num)| ((round + num) / 10, (round, num)))
                        .reduce(|_hash, input, output| {
                            println!("Input: {:?}", input);
                            // keep round-positive records as changes.
                            let ((round, record), count) = &input[0];
                            if *round > 0 {
                                output.push(((0, record.clone()), -*count));
                                output.push(((*round, record.clone()), *count));
                            }
                            // if any losers, increment their rounds.
                            for ((round, record), count) in input[1..].iter() {
                                output.push(((0, record.clone()), -*count));
                                output.push(((*round+1, record.clone()), *count));
                            }
                        })
                        .inspect(|x| println!("{:?}", x))
                        .map(|(_hash, pair)| pair)
                )
                .concat(&init)
                .map(|(round, num)| { (num, (round + num) / 10) })
                .map(|(_data, id)| id)
                .threshold(|_id,cnt| if cnt > &1 { *cnt } else { 0 })
                .assert_empty();
        });
    }
}