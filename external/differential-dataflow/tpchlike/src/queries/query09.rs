use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     nation,
//     o_year,
//     sum(amount) as sum_profit
// from
//     (
//         select
//             n_name as nation,
//             extract(year from o_orderdate) as o_year,
//             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
//         from
//             part,
//             supplier,
//             lineitem,
//             partsupp,
//             orders,
//             nation
//         where
//             s_suppkey = l_suppkey
//             and ps_suppkey = l_suppkey
//             and ps_partkey = l_partkey
//             and p_partkey = l_partkey
//             and o_orderkey = l_orderkey
//             and s_nationkey = n_nationkey
//             and p_name like '%:1%'
//     ) as profit
// group by
//     nation,
//     o_year
// order by
//     nation,
//     o_year desc;
// :n -1

fn substring(source: &[u8], query: &[u8]) -> bool {
    (0 .. (source.len() - query.len())).any(|offset|
        (0 .. query.len()).all(|i| source[i + offset] == query[i])
    )
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    println!("TODO: Q09 join order may be pessimal; could pivot to put lineitems last");

    let parts =
    collections
        .parts()
        .flat_map(|x| if substring(&x.name.as_bytes(), b"green") { Some(x.part_key) } else { None } );

    collections
        .lineitems()
        .map(|l| (l.part_key, (l.supp_key, l.order_key, l.extended_price * (100 - l.discount) / 100, l.quantity)))
        .semijoin(&parts)
        .map(|(part_key, (supp_key, order_key, revenue, quantity))| ((part_key, supp_key), (order_key, revenue, quantity)))
        .join(&collections.partsupps().map(|ps| ((ps.part_key, ps.supp_key), ps.supplycost)))
        .explode(|((_part_key, supp_key), ((order_key, revenue, quantity), supplycost))|
            Some(((order_key, supp_key), ((revenue - supplycost * quantity) as isize)))
        )
        .join_map(&collections.orders().map(|o| (o.order_key, o.order_date >> 16)), |_, &supp_key, &order_year| (supp_key, order_year))
        .join_map(&collections.suppliers().map(|s| (s.supp_key, s.nation_key)), |_, &order_year, &nation_key| (nation_key, order_year))
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

    experiment
        .lineitem(scope)
        .map(|l| (l.part_key, (l.supp_key, l.order_key, l.extended_price * (100 - l.discount) / 100, l.quantity)))
        .join_core(&arrangements.part, |&pk,&(sk,ok,ep,qu),p| {
            if substring(&p.name.as_bytes(), b"green") {
                Some(((pk,sk),(ok,ep,qu)))
            }
            else { None }
        })
        .join_core(&arrangements.partsupp, |&(_pk,sk),&(ok,ep,qu),s| Some((ok, (sk, ep - (qu * s.supplycost)))))
        .explode(|(ok,(sk,am))| Some(((ok,sk), am as isize)))
        .join_core(&arrangements.order, |_ok,&sk,o| Some((sk,o.order_date >> 16)))
        .join_core(&arrangements.supplier, |_sk,&yr,s| Some((s.nation_key, yr)))
        .join_core(&arrangements.nation, |_nk,&yr,n| Some((n.name,yr)))
        .count_total()
        .probe_with(probe);
}