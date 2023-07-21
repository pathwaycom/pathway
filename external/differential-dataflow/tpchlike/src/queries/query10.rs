use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Returned Item Reporting Query (Q10)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     c_custkey,
//     c_name,
//     sum(l_extendedprice * (1 - l_discount)) as revenue,
//     c_acctbal,
//     n_name,
//     c_address,
//     c_phone,
//     c_comment
// from
//     customer,
//     orders,
//     lineitem,
//     nation
// where
//     c_custkey = o_custkey
//     and l_orderkey = o_orderkey
//     and o_orderdate >= date ':1'
//     and o_orderdate < date ':1' + interval '3' month
//     and l_returnflag = 'R'
//     and c_nationkey = n_nationkey
// group by
//     c_custkey,
//     c_name,
//     c_acctbal,
//     c_phone,
//     n_name,
//     c_address,
//     c_comment
// order by
//     revenue desc;
// :n 20

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let lineitems =
    collections
        .lineitems()
        .explode(|x|
            if starts_with(&x.return_flag, b"R") {
                Some((x.order_key, (x.extended_price * (100 - x.discount)) as isize))
            }
            else { None }
        );

    let orders =
    collections
        .orders()
        .flat_map(|o|
            if create_date(1993,10,1) < o.order_date && o.order_date <= create_date(1994,1,1) {
                Some((o.order_key, o.cust_key))
            }
            else { None }
        )
        .semijoin(&lineitems)
        .map(|(_, cust_key)| cust_key);

    collections
        .customers()
        .map(|c| (c.cust_key, (c.name, c.phone, c.address, c.comment, c.nation_key)))
        .semijoin(&orders)
        .map(|(cust_key, (name, phn, addr, comm, nation_key))| (nation_key, (cust_key, name, phn, addr, comm)))
        .join(&collections.nations().map(|n| (n.nation_key, n.name)))
        .count_total()
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

    use differential_dataflow::operators::arrange::ArrangeBySelf;

    experiment
        .lineitem(scope)
        .explode(|x|
            if starts_with(&x.return_flag, b"R") {
                Some((x.order_key, (x.extended_price * (100 - x.discount)) as isize))
            }
            else { None }
        )
        .arrange_by_self()
        .join_core(&arrangements.order, |_ok,&(),o| {
            if create_date(1993,10,1) < o.order_date && o.order_date <= create_date(1994,1,1) {
                Some(o.cust_key)
            }
            else { None }
        })
        .arrange_by_self()
        .join_core(&arrangements.customer, |&ck,&(),c| {
            Some((c.nation_key, (ck,c.name,c.acctbal,c.address,c.phone,c.comment)))
        })
        .join_core(&arrangements.nation, |_nk,&data,n| Some((n.name, data)))
        .count_total()
        .probe_with(probe);
}