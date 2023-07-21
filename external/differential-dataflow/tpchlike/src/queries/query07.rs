use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Volume Shipping Query (Q7)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     supp_nation,
//     cust_nation,
//     l_year,
//     sum(volume) as revenue
// from
//     (
//         select
//             n1.n_name as supp_nation,
//             n2.n_name as cust_nation,
//             extract(year from l_shipdate) as l_year,
//             l_extendedprice * (1 - l_discount) as volume
//         from
//             supplier,
//             lineitem,
//             orders,
//             customer,
//             nation n1,
//             nation n2
//         where
//             s_suppkey = l_suppkey
//             and o_orderkey = l_orderkey
//             and c_custkey = o_custkey
//             and s_nationkey = n1.n_nationkey
//             and c_nationkey = n2.n_nationkey
//             and (
//                 (n1.n_name = ':1' and n2.n_name = ':2')
//                 or (n1.n_name = ':2' and n2.n_name = ':1')
//             )
//             and l_shipdate between date '1995-01-01' and date '1996-12-31'
//     ) as shipping
// group by
//     supp_nation,
//     cust_nation,
//     l_year
// order by
//     supp_nation,
//     cust_nation,
//     l_year;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    println!("TODO: Q07 could use `join_core` to fuse map and filter");

    let nations =
    collections
        .nations()
        .filter(|n| starts_with(&n.name, b"FRANCE") || starts_with(&n.name, b"GERMANY"))
        .map(|n| (n.nation_key, n.name));

    let customers =
    collections
        .customers()
        .map(|c| (c.nation_key, c.cust_key))
        .join_map(&nations, |_, &cust_key, &name| (cust_key, name));

    let orders =
    collections
        .orders()
        .map(|o| (o.cust_key, o.order_key))
        .join_map(&customers, |_, &order_key, &name| (order_key, name));

    let suppliers =
    collections
        .suppliers()
        .map(|s| (s.nation_key, s.supp_key))
        .join_map(&nations, |_, &supp_key, &name| (supp_key, name));

    collections
        .lineitems()
        .explode(|l|
            if create_date(1995, 1, 1) <= l.ship_date && l.ship_date <= create_date(1996, 12, 31) {
                Some(((l.supp_key, (l.order_key, l.ship_date)), (l.extended_price * (100 - l.discount)) as isize / 100))
            }
            else { None }
        )
        .join_map(&suppliers, |_, &(order_key, ship_date), &name_s| (order_key, (ship_date, name_s)))
        .join_map(&orders, |_, &(ship_date, name_s), &name_c| (name_s, name_c, ship_date >> 16))
        .filter(|x| x.0 != x.1)
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
            if create_date(1995, 1, 1) <= l.ship_date && l.ship_date <= create_date(1996, 12, 31) {
                Some(((l.supp_key, (l.order_key, l.ship_date >> 16)), (l.extended_price * (100 - l.discount)) as isize / 100))
            }
            else { None }
        )
        .join_core(&arrangements.supplier, |_sk,&(ok,sd),s| Some((s.nation_key,(ok,sd))))
        .join_core(&arrangements.nation, |_nk,&(ok,sd),n| {
            if starts_with(&n.name, b"FRANCE") || starts_with(&n.name, b"GERMANY") {
                Some((ok,(sd,n.name)))
            }
            else {
                None
            }
        })
        .join_core(&arrangements.order, |_ok,&(sd,n1),o| Some((o.cust_key,(sd,n1))))
        .join_core(&arrangements.customer, |_ck,&(sd,n1),c| Some((c.nation_key,(sd,n1))))
        .join_core(&arrangements.nation, |_nk,&(sd,n1),n| {
            if starts_with(&n.name, b"FRANCE") || starts_with(&n.name, b"GERMANY") {
                Some((sd,n1,n.name))
            }
            else {
                None
            }
        })
        .filter(|&(_sd,n1,n2)| n1 != n2)
        .count_total()
        .probe_with(probe);
}