use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     s_name,
//     count(*) as numwait
// from
//     supplier,
//     lineitem l1,
//     orders,
//     nation
// where
//     s_suppkey = l1.l_suppkey
//     and o_orderkey = l1.l_orderkey
//     and o_orderstatus = 'F'
//     and l1.l_receiptdate > l1.l_commitdate
//     and exists (
//         select
//             *
//         from
//             lineitem l2
//         where
//             l2.l_orderkey = l1.l_orderkey
//             and l2.l_suppkey <> l1.l_suppkey
//     )
//     and not exists (
//         select
//             *
//         from
//             lineitem l3
//         where
//             l3.l_orderkey = l1.l_orderkey
//             and l3.l_suppkey <> l1.l_suppkey
//             and l3.l_receiptdate > l3.l_commitdate
//     )
//     and s_nationkey = n_nationkey
//     and n_name = ':1'
// group by
//     s_name
// order by
//     numwait desc,
//     s_name;
// :n 100


fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let orders =
    collections
        .orders()
        .flat_map(|o|
            if starts_with(&o.order_status, b"F") { Some(o.order_key) }
            else { None }
        );

    // lineitems relevant to "F" orders.
    let lineitems =
    collections
        .lineitems()
        .map(|l| (l.order_key, (l.supp_key, l.receipt_date > l.commit_date)))
        .semijoin(&orders);

    let lateitems = lineitems.filter(|l| (l.1).1);
    let lateorders = lateitems.map(|l| l.0).distinct_total();

    let problems =
    lineitems
        .map(|(order_key, (_supp_key, is_late))| (order_key, is_late))
        .semijoin(&lateorders)    //- on_time and late, but just one late -\\
        .reduce(|_order_key, s, t| if s.len() == 2 && s[1].1 == 1 { t.push(((), 1)); })
        .map(|(order_key, _)| order_key);

    let latesupps =
    lateitems
        .semijoin(&problems)
        .map(|(_order_key, (supp_key, _))| supp_key);

    collections
        .suppliers()
        .map(|s| (s.supp_key, (s.name, s.nation_key)))
        .semijoin(&latesupps)
        .map(|(_, (name, nation))| (nation, name))
        .semijoin(&collections.nations().filter(|n| starts_with(&n.name, b"SAUDI ARABIA")).map(|n| n.nation_key))
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

    experiment
        .lineitem(scope)
        .map(|l| (l.order_key, (l.receipt_date > l.commit_date, l.supp_key)))
        .reduce(|_ok,input,output| {

            let late_supp = (input[0].0).1;
            let only_late = input[1..].iter().all(|(x,_)| !x.0);
            let other_supp = input[1..].iter().any(|(x,_)| x.1 != late_supp);

            if only_late && other_supp {
                output.push((late_supp, 1));
            }

        })
        .join_core(&arrangements.order, |_ok,&sk,o| {
            if starts_with(&o.order_status, b"F") { Some((sk, ())) } else { None }
        })
        .join_core(&arrangements.supplier, |_sk,&(),s| Some((s.nation_key, s.name)))
        .join_core(&arrangements.nation, |_nk,&nm,n|
            if starts_with(&n.name, b"SAUDI ARABIA") { Some(nm) } else { None }
        )
        .count_total()
        .probe_with(probe);
}