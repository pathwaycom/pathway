use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Shipping Priority Query (Q3)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     l_orderkey,
//     sum(l_extendedprice * (1 - l_discount)) as revenue,
//     o_orderdate,
//     o_shippriority
// from
//     customer,
//     orders,
//     lineitem
// where
//     c_mktsegment = ':1'
//     and c_custkey = o_custkey
//     and l_orderkey = o_orderkey
//     and o_orderdate < date ':2'
//     and l_shipdate > date ':2'
// group by
//     l_orderkey,
//     o_orderdate,
//     o_shippriority
// order by
//     revenue desc,
//     o_orderdate;
// :n 10

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let customers =
    collections
        .customers()
        .flat_map(|c| if starts_with(&c.mktsegment[..], b"BUILDING") { Some(c.cust_key) } else { None });

    let lineitems =
    collections
        .lineitems()
        .explode(|l|
            if l.ship_date > create_date(1995, 3, 15) {
                Some((l.order_key, (l.extended_price * (100 - l.discount) / 100) as isize))
            }
            else { None }
        );

    let orders =
    collections
        .orders()
        .filter(|o| o.order_date < create_date(1995, 3, 15))
        .map(|o| (o.cust_key, (o.order_key, o.order_date, o.ship_priority)));

    orders
        .semijoin(&customers)
        .map(|(_, (order_key, order_date, ship_priority))| (order_key, (order_date, ship_priority)))
        .semijoin(&lineitems)
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
        .explode(|l|
            if l.ship_date > create_date(1995, 3, 15) {
                Some(((l.order_key, ()), (l.extended_price * (100 - l.discount) / 100) as isize))
            }
            else { None }
        )
        .join_core(&arrangements.order, |_k, &(), o| {
            if o.order_date < create_date(1995, 3, 15) {
                Some((o.cust_key, (o.order_key, o.order_date, o.ship_priority)))
            }
            else {
                None
            }
        })
        .join_core(&arrangements.customer, |_k,o,c| {
            if starts_with(&c.mktsegment[..], b"BUILDING") {
                Some(o.clone())
            }
            else {
                None
            }
        })
        .count_total()
        .probe_with(probe);
}