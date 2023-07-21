use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     s_acctbal,
//     s_name,
//     n_name,
//     p_partkey,
//     p_mfgr,
//     s_address,
//     s_phone,
//     s_comment
// from
//     part,
//     supplier,
//     partsupp,
//     nation,
//     region
// where
//     p_partkey = ps_partkey
//     and s_suppkey = ps_suppkey
//     and p_size = :1
//     and p_type like '%:2'
//     and s_nationkey = n_nationkey
//     and n_regionkey = r_regionkey
//     and r_name = ':3'
//     and ps_supplycost = (
//         select
//             min(ps_supplycost)
//         from
//             partsupp,
//             supplier,
//             nation,
//             region
//         where
//             p_partkey = ps_partkey
//             and s_suppkey = ps_suppkey
//             and s_nationkey = n_nationkey
//             and n_regionkey = r_regionkey
//             and r_name = ':3'
//     )
// order by
//     s_acctbal desc,
//     n_name,
//     s_name,
//     p_partkey;
// :n 100

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

fn substring(source: &[u8], query: &[u8]) -> bool {
    (0 .. (source.len() - query.len())).any(|offset|
        (0 .. query.len()).all(|i| source[i + offset] == query[i])
    )
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let regions =
    collections
        .regions()
        .flat_map(|x| if starts_with(&x.name[..], b"EUROPE") { Some(x.region_key) } else { None });

    let nations =
    collections
        .nations()
        .map(|x| (x.region_key, (x.nation_key, x.name)))
        .semijoin(&regions)
        .map(|(_region_key, (nation_key, name))| (nation_key, name));

    let suppliers =
    collections
        .suppliers()
        .map(|x| (x.nation_key, (x.acctbal, x.name, x.address, x.phone, x.comment, x.supp_key)))
        .semijoin(&nations.map(|x| x.0))
        .map(|(nat, (acc, nam, add, phn, com, key))| (key, (nat, acc, nam, add, phn, com)));

    let parts =
    collections
        .parts()
        .flat_map(|x| if substring(x.typ.as_str().as_bytes(), b"BRASS") && x.size == 15 { Some((x.part_key, x.mfgr)) } else { None });

    let partsupps =
    collections
        .partsupps()
        .map(|x| (x.supp_key, (x.part_key, x.supplycost)))
        .semijoin(&suppliers.map(|x| x.0))
        .map(|(supp, (part, supply_cost))| (part, (supply_cost, supp)))
        .semijoin(&parts.map(|x| x.0))
        .reduce(|_x, s, t| {
            let minimum = (s[0].0).0;
            t.extend(s.iter().take_while(|x| (x.0).0 == minimum).map(|&(&x,w)| (x,w)));
        });

    partsupps
        .join(&parts)
        .map(|(part_key, ((cost, supp), mfgr))| (supp, (cost, part_key, mfgr)))
        .join(&suppliers)
        .map(|(_supp, ((cost, part, mfgr), (nat, acc, nam, add, phn, com)))| (nat, (cost, part, mfgr, acc, nam, add, phn, com)))
        .join(&nations)
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

    let relevant_suppliers =
    arrangements
        .partsupp
        .as_collection(|_,x| (x.supp_key, (x.part_key, x.supplycost)))
        .inner
        .map(move |(d,t,r)| (d, ::std::cmp::max(t,round),r))
        .as_collection()
        .join_core(&arrangements.supplier, |&sk, &(pk,sc), s| Some((s.nation_key, (pk,sc,sk))))
        .join_core(&arrangements.nation, |_nk, &(pk,sc,sk), n| Some((n.region_key, (pk,sc,sk,n.name))))
        .join_core(&arrangements.region, |_rk, &(pk,sc,sk,nm), r| {
            if starts_with(&r.name[..], b"EUROPE") { Some((pk,(sc,sk,nm))) } else { None }
        });

    let cheapest_suppliers =
    relevant_suppliers
        .reduce(|_pk, s, t| {
            let minimum = (s[0].0).0;
            t.extend(s.iter().take_while(|x| (x.0).0 == minimum).map(|&(&x,w)| (x,w)));
        });

    cheapest_suppliers
        .join_core(&arrangements.part, |&pk,&(sc,sk,nm),p| {
            if substring(p.typ.as_str().as_bytes(), b"BRASS") && p.size == 15 {
                Some((sk, (sc,pk,nm,p.mfgr)))
            }
            else {
                None
            }
        })
        .join_core(&arrangements.supplier, |_sk,&(sc,pk,nm,pm),s| {
            Some((sc,pk,nm,pm,s.acctbal,s.name,s.address,s.phone,s.comment))
        })
        .probe_with(probe);
}