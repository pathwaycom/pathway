use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R National Market Share Query (Q8)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     o_year,
//     sum(case
//         when nation = ':1' then volume
//         else 0
//     end) / sum(volume) as mkt_share
// from
//     (
//         select
//             extract(year from o_orderdate) as o_year,
//             l_extendedprice * (1 - l_discount) as volume,
//             n2.n_name as nation
//         from
//             part,
//             supplier,
//             lineitem,
//             orders,
//             customer,
//             nation n1,
//             nation n2,
//             region
//         where
//             p_partkey = l_partkey
//             and s_suppkey = l_suppkey
//             and l_orderkey = o_orderkey
//             and o_custkey = c_custkey
//             and c_nationkey = n1.n_nationkey
//             and n1.n_regionkey = r_regionkey
//             and r_name = ':2'
//             and s_nationkey = n2.n_nationkey
//             and o_orderdate between date '1995-01-01' and date '1996-12-31'
//             and p_type = ':3'
//     ) as all_nations
// group by
//     o_year
// order by
//     o_year;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let regions = collections.regions().filter(|r| starts_with(&r.name, b"AMERICA")).map(|r| r.region_key);
    let nations1 = collections.nations().map(|n| (n.region_key, n.nation_key)).semijoin(&regions).map(|x| x.1);
    let customers = collections.customers().map(|c| (c.nation_key, c.cust_key)).semijoin(&nations1).map(|x| x.1);
    let orders =
    collections
        .orders()
        .flat_map(|o|
            if create_date(1995,1,1) <= o.order_date && o.order_date <= create_date(1996, 12, 31) {
                Some((o.cust_key, (o.order_key, o.order_date >> 16)))
            }
            else { None }
        )
        .semijoin(&customers)
        .map(|x| x.1);

    let nations2 = collections.nations.map(|n| (n.nation_key, starts_with(&n.name, b"BRAZIL")));
    let suppliers =
    collections
        .suppliers()
        .map(|s| (s.nation_key, s.supp_key))
        .join(&nations2)
        .map(|(_, (supp_key, is_name))| (supp_key, is_name));

    let parts = collections.parts().filter(|p| p.typ.as_str() == "ECONOMY ANODIZED STEEL").map(|p| p.part_key);

    collections
        .lineitems()
        .explode(|l| Some(((l.part_key, (l.supp_key, l.order_key)), ((l.extended_price * (100 - l.discount)) as isize / 100))))
        .semijoin(&parts)
        .map(|(_part_key, (supp_key, order_key))| (order_key, supp_key))
        .join(&orders)
        .map(|(_order_key, (supp_key, order_date))| (supp_key, order_date))
        .join(&suppliers)
        .explode(|(_, (order_date, is_name))| Some((order_date, DiffPair::new(if is_name { 1 } else { 0 }, 1))))
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
        .explode(|l| Some(((l.part_key, (l.order_key, l.supp_key)), ((l.extended_price * (100 - l.discount)) as isize / 100))))
        .join_core(&arrangements.part, |_pk,&(ok,sk),p| {
            if p.typ.as_str() == "ECONOMY ANODIZED STEEL" { Some((ok,sk)) } else { None }
        })
        .join_core(&arrangements.order, |_ok,&sk,o| {
            if create_date(1995,1,1) <= o.order_date && o.order_date <= create_date(1996, 12, 31) {
                Some((o.cust_key, (sk,o.order_date >> 16)))
            }
            else { None }
        })
        .join_core(&arrangements.customer, |_ck,&(sk,yr),c| Some((c.nation_key, (sk,yr))))
        .join_core(&arrangements.nation, |_nk,&(sk,yr),n| Some((n.region_key, (sk,yr))))
        .join_core(&arrangements.region, |_rk,&(sk,yr),r| {
            if starts_with(&r.name, b"AMERICA") { Some((sk,yr,true)) } else { Some((sk,yr,false)) }
        })
        .explode(|(sk,yr,is_name)| Some(((sk,yr), DiffPair::new(if is_name { 1 } else { 0 }, 1))))
        .join_core(&arrangements.supplier, |_sk,&yr,s| Some((s.nation_key, yr)))
        .join_core(&arrangements.nation, |_nk,&yr,n| Some((n.name,yr)))
        .count_total()
        .probe_with(probe);
}