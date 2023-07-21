use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Potential Part Promotion Query (Q20)
// -- Function Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     s_name,
//     s_address
// from
//     supplier,
//     nation
// where
//     s_suppkey in (
//         select
//             ps_suppkey
//         from
//             partsupp
//         where
//             ps_partkey in (
//                 select
//                     p_partkey
//                 from
//                     part
//                 where
//                     p_name like ':1%'
//             )
//             and ps_availqty > (
//                 select
//                     0.5 * sum(l_quantity)
//                 from
//                     lineitem
//                 where
//                     l_partkey = ps_partkey
//                     and l_suppkey = ps_suppkey
//                     and l_shipdate >= date ':2'
//                     and l_shipdate < date ':2' + interval '1' year
//             )
//     )
//     and s_nationkey = n_nationkey
//     and n_name = ':3'
// order by
//     s_name;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    println!("TODO: Q20 uses a `reduce_abelian` to get an arrangement, but could use `count_total`");

    let partkeys = collections.parts.filter(|p| p.name.as_bytes() == b"forest").map(|p| p.part_key);

    let available =
    collections
        .lineitems()
        .flat_map(|l|
            if l.ship_date >= create_date(1994, 1, 1) && l.ship_date < create_date(1995, 1, 1) {
                Some((l.part_key, (l.supp_key, l.quantity)))
            }
            else { None }
        )
        .semijoin(&partkeys)
        .explode(|l| Some(((((l.0 as u64) << 32) + (l.1).0 as u64, ()), (l.1).1 as isize)))
        .reduce_abelian::<_,DefaultValTrace<_,_,_,_>>("Reduce", |_k,s,t| t.push((s[0].1, 1)));

    let suppliers =
    collections
        .partsupps()
        .map(|ps| (ps.part_key, (ps.supp_key, ps.availqty)))
        .semijoin(&partkeys)
        .map(|(part_key, (supp_key, avail))| (((part_key as u64) << 32) + (supp_key as u64), avail))
        .join_core(&available, |&key, &avail1, &avail2| {
            let key: u64 = key;
            let avail2: isize = avail2;
            if avail1 > avail2 as i32 / 2 {
                Some((key & (u32::max_value() as u64)) as usize)
            }
            else { None }
        });

    let nations = collections.nations.filter(|n| starts_with(&n.name, b"CANADA")).map(|n| (n.nation_key, n.name));

    collections
        .suppliers()
        .map(|s| (s.supp_key, (s.name, s.address, s.nation_key)))
        .semijoin(&suppliers)
        .map(|(_, (name, addr, nation))| (nation, (name, addr)))
        .join(&nations)
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
            if l.ship_date >= create_date(1994, 1, 1) && l.ship_date < create_date(1995, 1, 1) {
                Some(((l.part_key, l.supp_key), l.quantity as isize))
            }
            else { None }
        )
        .join_core(&arrangements.part, |&pk,&sk,p|
            if p.name.as_bytes() == b"forest" { Some((pk,sk)) } else { None }
        )
        .count_total()
        .join_core(&arrangements.partsupp, |&(_pk,sk),tot,ps| {
            if (ps.availqty as isize) > tot / 2 { Some((sk, ())) } else { None }
        })
        .distinct_total()
        .join_core(&arrangements.supplier, |_sk,&(),s| Some((s.nation_key, (s.name, s.address))))
        .join_core(&arrangements.nation, |_nk,&(nm,ad),n|
            if starts_with(&n.name, b"CANADA") { Some((nm,ad)) } else { None }
        )
        .probe_with(probe);
}