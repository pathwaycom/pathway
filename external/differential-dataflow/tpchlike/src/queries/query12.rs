use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::{ArrangeBySelf, ArrangeByKey};
use differential_dataflow::difference::DiffPair;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     l_shipmode,
//     sum(case
//         when o_orderpriority = '1-URGENT'
//             or o_orderpriority = '2-HIGH'
//             then 1
//         else 0
//     end) as high_line_count,
//     sum(case
//         when o_orderpriority <> '1-URGENT'
//             and o_orderpriority <> '2-HIGH'
//             then 1
//         else 0
//     end) as low_line_count
// from
//     orders,
//     lineitem
// where
//     o_orderkey = l_orderkey
//     and l_shipmode in (':1', ':2')
//     and l_commitdate < l_receiptdate
//     and l_shipdate < l_commitdate
//     and l_receiptdate >= date ':3'
//     and l_receiptdate < date ':3' + interval '1' year
// group by
//     l_shipmode
// order by
//     l_shipmode;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    println!("TODO: Q12 does contortions because isize doesn't implement Mul<DiffPair<isize, isize>>.");

    let orders =
    collections
        .orders()
        .explode(|o|
            if starts_with(&o.order_priority, b"1-URGENT") || starts_with(&o.order_priority, b"2-HIGH") {
                Some((o.order_key, DiffPair::new(1, 0)))
            }
            else {
                Some((o.order_key, DiffPair::new(0, 1)))
            }
        )
        .arrange_by_self();

    let lineitems =
    collections
        .lineitems()
        .flat_map(|l|
            if (starts_with(&l.ship_mode, b"MAIL") || starts_with(&l.ship_mode, b"SHIP")) &&
                l.commit_date < l.receipt_date && l.ship_date < l.commit_date &&
                create_date(1994,1,1) <= l.receipt_date && l.receipt_date < create_date(1995,1,1) {
                Some((l.order_key, l.ship_mode))
            }
            else { None }
        )
        .arrange_by_key();

    orders
        .join_core(&lineitems, |_, _, &ship_mode| Some(ship_mode))
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
        .flat_map(|l|
            if (starts_with(&l.ship_mode, b"MAIL") || starts_with(&l.ship_mode, b"SHIP")) &&
                l.commit_date < l.receipt_date && l.ship_date < l.commit_date &&
                create_date(1994,1,1) <= l.receipt_date && l.receipt_date < create_date(1995,1,1) {
                Some((l.order_key, l.ship_mode))
            }
            else { None }
        )
        .join_core(&arrangements.order, |_ok,&sm,o| {
            Some((sm, starts_with(&o.order_priority, b"1-URGENT") || starts_with(&o.order_priority, b"2-HIGH")))
        })
        .explode(|(sm,priority)| Some((sm, if priority { DiffPair::new(1, 0) } else { DiffPair::new(1, 0) })))
        .count_total()
        .probe_with(probe);
}