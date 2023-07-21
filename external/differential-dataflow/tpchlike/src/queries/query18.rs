use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Large Volume Customer Query (Q18)
// -- Function Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     c_name,
//     c_custkey,
//     o_orderkey,
//     o_orderdate,
//     o_totalprice,
//     sum(l_quantity)
// from
//     customer,
//     orders,
//     lineitem
// where
//     o_orderkey in (
//         select
//             l_orderkey
//         from
//             lineitem
//         group by
//             l_orderkey having
//                 sum(l_quantity) > :1
//     )
//     and c_custkey = o_custkey
//     and o_orderkey = l_orderkey
// group by
//     c_name,
//     c_custkey,
//     o_orderkey,
//     o_orderdate,
//     o_totalprice
// order by
//     o_totalprice desc,
//     o_orderdate;
// :n 100

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let orders =
    collections
        .orders()
        .map(|o| (o.order_key, (o.cust_key, o.order_date, o.total_price)));

    collections
        .lineitems()
        .explode(|l| Some((l.order_key, l.quantity as isize)))
        .count_total()
        .filter(|&(_key, cnt)| cnt > 300)
        .join_map(&orders, |&o_key, &quant, &(cust_key, date, price)| (cust_key, (o_key, date, price, quant)))
        .join(&collections.customers().map(|c| (c.cust_key, c.name)))
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
        .explode(|l| Some((l.order_key, l.quantity as isize)))
        .count_total()
        .filter(|&(_key, cnt)| cnt > 300)
        .join_core(&arrangements.order, |_ok,&cnt,o| Some((o.cust_key, (o.order_date, o.total_price, cnt))))
        .join_core(&arrangements.customer, |&ck,&(od,tp,cnt),c| Some((ck,c.name,od,tp,cnt)))
        .count_total()
        .probe_with(probe);
}