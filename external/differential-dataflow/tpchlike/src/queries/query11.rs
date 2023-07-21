use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Important Stock Identification Query (Q11)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     ps_partkey,
//     sum(ps_supplycost * ps_availqty) as value
// from
//     partsupp,
//     supplier,
//     nation
// where
//     ps_suppkey = s_suppkey
//     and s_nationkey = n_nationkey
//     and n_name = ':1'
// group by
//     ps_partkey having
//         sum(ps_supplycost * ps_availqty) > (
//             select
//                 sum(ps_supplycost * ps_availqty) * :2
//             from
//                 partsupp,
//                 supplier,
//                 nation
//             where
//                 ps_suppkey = s_suppkey
//                 and s_nationkey = n_nationkey
//                 and n_name = ':1'
//         )
// order by
//     value desc;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let nations =
    collections
        .nations()
        .filter(|n| starts_with(&n.name, b"GERMANY"))
        .map(|n| n.nation_key);

    let suppliers =
    collections
        .suppliers()
        .map(|s| (s.nation_key, s.supp_key))
        .semijoin(&nations)
        .map(|s| s.1);

    collections
        .partsupps()
        .explode(|x| Some(((x.supp_key, x.part_key), (x.supplycost as isize) * (x.availqty as isize))))
        .semijoin(&suppliers)
        .map(|(_, part_key)| ((), part_key))
        .reduce(|_part_key, s, t| {
            let threshold: isize = s.iter().map(|x| x.1 as isize).sum::<isize>() / 10000;
            t.extend(s.iter().filter(|x| x.1 > threshold).map(|&(&a,b)| (a, b)));
        })
        .map(|(_, part_key)| part_key)
        .count_total()
        .probe_with(probe);
}

pub fn query_arranged<G: Scope<Timestamp=usize>>(
    scope: &mut G,
    probe: &mut ProbeHandle<usize>,
    experiment: &mut Experiment,
    arrangements: &mut Arrangements,
    round: usize,
)
where
    G::Timestamp: Lattice+TotalOrder+Ord
{
    use timely::dataflow::operators::Map;
    use differential_dataflow::AsCollection;

    let arrangements = arrangements.in_scope(scope, experiment);

    arrangements
        .partsupp
        .as_collection(|_,ps| ((ps.supp_key, ps.part_key), (ps.supplycost as isize) * (ps.availqty as isize)))
        .inner
        .map(move |(d,t,r)| (d, ::std::cmp::max(t,round),r))
        .as_collection()
        .explode(|((sk,pk),prod)| Some(((sk,pk),prod)))
        .join_core(&arrangements.supplier, |_sk,&pk,s| Some((s.nation_key, pk)))
        .join_core(&arrangements.nation, |_nk,&pk,n|
            if starts_with(&n.name, b"GERMANY") { Some(((), pk)) } else { None }
        )
        .reduce(|_part_key, s, t| {
            let threshold: isize = s.iter().map(|x| x.1 as isize).sum::<isize>() / 10000;
            t.extend(s.iter().filter(|x| x.1 > threshold).map(|&(&a,b)| (a, b)));
        })
        .map(|(_, part_key)| part_key)
        .count_total()
        .probe_with(probe);
}