use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     sum(l_extendedprice * l_discount) as revenue
// from
//     lineitem
// where
//     l_shipdate >= date ':1'
//     and l_shipdate < date ':1' + interval '1' year
//     and l_discount between :2 - 0.01 and :2 + 0.01
//     and l_quantity < :3;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    collections
        .lineitems()
        .explode(|x| {
            if (create_date(1994, 1, 1) <= x.ship_date) && (x.ship_date < create_date(1995, 1, 1)) && (5 <= x.discount) && (x.discount <= 7) && (x.quantity < 24) {
                Some(((), (x.extended_price * x.discount / 100) as isize))
            }
            else { None }
        })
        .count_total()
        // .inspect(|x| println!("{:?}", x))
        .probe_with(probe);
}

pub fn query_arranged<G: Scope<Timestamp=usize>>(
    scope: &mut G,
    probe: &mut ProbeHandle<usize>,
    experiment: &mut Experiment,
    _arrangements: &mut Arrangements,
)
where
    G::Timestamp: Lattice+TotalOrder+Ord
{
    experiment
        .lineitem(scope)
        .explode(|x| {
            if (create_date(1994, 1, 1) <= x.ship_date) && (x.ship_date < create_date(1995, 1, 1)) && (5 <= x.discount) && (x.discount <= 7) && (x.quantity < 24) {
                Some(((), (x.extended_price * x.discount / 100) as isize))
            }
            else { None }
        })
        .count_total()
        .probe_with(probe);
}