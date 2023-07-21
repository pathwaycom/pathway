use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//   sum(l_extendedprice) / 7.0 as avg_yearly
// from
//   lineitem,
//   part
// where
//   p_partkey = l_partkey
//   and p_brand = ':1'
//   and p_container = ':2'
//   and l_quantity < (
//     select
//       0.2 * avg(l_quantity)
//     from
//       lineitem
//     where
//       l_partkey = p_partkey
//   );
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let parts =
    collections
        .parts()   // We fluff out search strings to have the right lengths. \\
        .flat_map(|x|  {
            if &x.brand[..8] == b"Brand#23" && &x.container[..7] == b"MED BOX" {
                Some(x.part_key)
            }
            else { None }
        });

    collections
        .lineitems()
        .map(|x| (x.part_key, (x.quantity, x.extended_price)))
        .semijoin(&parts)
        .reduce(|_k, s, t| {

            // determine the total and count of quantity.
            let total: i64 = s.iter().map(|x| (x.0).0 * (x.1 as i64)).sum();
            let count: i64 = s.iter().map(|x| x.1 as i64).sum();

            // produce as output those tuples with below-threshold quantity.
            t.extend(s.iter().filter(|&&(&(quantity,_),_)| 5 * quantity * count < total)
                             .map(|&(&(_,price),count)| (price, count)));
        })
        .explode(|(_part, price)| Some(((), price as isize)))
        .count_total()
        // .inspect(|x| println!("{:?}", x))
        .probe_with(probe);
}

pub fn query_arranged<G: Scope<Timestamp=usize>>(
    scope: &mut G,
    probe: &mut ProbeHandle<usize>,
    experiment: &mut Experiment,
    arrangements: &mut Arrangements,
)
where
    G::Timestamp: Lattice+TotalOrder+Ord
{
    let arrangements = arrangements.in_scope(scope, experiment);

    experiment
        .lineitem(scope)
        .map(|x| (x.part_key, (x.quantity, x.extended_price)))
        .join_core(&arrangements.part, |&pk,&(qu,ep),p| {
            if &p.brand[..8] == b"Brand#23" && &p.container[..7] == b"MED BOX" {
                Some((pk,(qu,ep)))
            }
            else { None }

        })
        .reduce(|_k, s, t| {

            // determine the total and count of quantity.
            let total: i64 = s.iter().map(|x| (x.0).0 * (x.1 as i64)).sum();
            let count: i64 = s.iter().map(|x| x.1 as i64).sum();

            // produce as output those tuples with below-threshold quantity.
            t.extend(s.iter().filter(|&&(&(quantity,_),_)| 5 * quantity * count < total)
                             .map(|&(&(_,price),count)| (price, count)));
        })
        .explode(|(_part, price)| Some(((), price as isize)))
        .count_total()
        .probe_with(probe);
}