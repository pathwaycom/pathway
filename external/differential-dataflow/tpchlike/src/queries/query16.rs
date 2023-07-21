use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use regex::Regex;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     p_brand,
//     p_type,
//     p_size,
//     count(distinct ps_suppkey) as supplier_cnt
// from
//     partsupp,
//     part
// where
//     p_partkey = ps_partkey
//     and p_brand <> ':1'
//     and p_type not like ':2%'
//     and p_size in (:3, :4, :5, :6, :7, :8, :9, :10)
//     and ps_suppkey not in (
//         select
//             s_suppkey
//         from
//             supplier
//         where
//             s_comment like '%Customer%Complaints%'
//     )
// group by
//     p_brand,
//     p_type,
//     p_size
// order by
//     supplier_cnt desc,
//     p_brand,
//     p_type,
//     p_size;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let regex = Regex::new("Customer.*Complaints").expect("Regex construction failed");

    let suppliers =
    collections
        .suppliers()
        .flat_map(move |s| if regex.is_match(&s.comment) { Some(s.supp_key) } else { None } );

    let parts = collections
        .partsupps()
        .map(|ps| (ps.supp_key, ps.part_key))
        .antijoin(&suppliers)
        .map(|(_supp_key, part_key)| part_key);

    collections
        .parts()
        .flat_map(|p|
            if !starts_with(&p.brand, b"Brand#45") && !starts_with(&p.typ.as_bytes(), b"MEDIUM POLISHED") && [49, 14, 23, 45, 19, 3, 36, 9].contains(&p.size) {
                Some((p.part_key, (p.brand, p.typ, p.size)))
            }
            else { None }
        )
        .semijoin(&parts)
        .map(|(_, brand_type_size)| brand_type_size)
        .count_total()
        // .inspect(|x| println!("{:?}", x))
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

    let regex = Regex::new("Customer.*Complaints").expect("Regex construction failed");

    let suppliers =
    arrangements
        .supplier
        .flat_map_ref(move |_,s| if regex.is_match(&s.comment) { Some(s.supp_key) } else { None } )
        .inner
        .map(move |(d,t,r)| (d, ::std::cmp::max(t,round),r))
        .as_collection();

    arrangements
        .partsupp
        .as_collection(|&psk,_| psk)
        .inner
        .map(move |(d,t,r)| (d, ::std::cmp::max(t,round),r))
        .as_collection()
        .join_core(&arrangements.part, |_pk,&sk,p| {
            if !starts_with(&p.brand, b"Brand#45") && !starts_with(&p.typ.as_bytes(), b"MEDIUM POLISHED") && [49, 14, 23, 45, 19, 3, 36, 9].contains(&p.size) {
                Some((sk, (p.brand, p.typ, p.size)))
            }
            else { None }

        })
        .antijoin(&suppliers)
        .map(|(_sk, stuff)| stuff)
        .count_total()
        .probe_with(probe);
}