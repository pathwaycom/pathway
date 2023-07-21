use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Promotion Effect Query (Q14)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     100.00 * sum(case
//         when p_type like 'PROMO%'
//             then l_extendedprice * (1 - l_discount)
//         else 0
//     end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
// from
//     lineitem,
//     part
// where
//     l_partkey = p_partkey
//     and l_shipdate >= date ':1'
//     and l_shipdate < date ':1' + interval '1' month;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let lineitems =
    collections
        .lineitems()
        .explode(|l|
            if create_date(1995,9,1) <= l.ship_date && l.ship_date < create_date(1995,10,1) {
                Some((l.part_key, (l.extended_price * (100 - l.discount) / 100) as isize ))
            }
            else { None }
        )
        .arrange_by_self();

    collections
        .parts()
        .explode(|p| Some((p.part_key, DiffPair::new(1, if starts_with(&p.typ.as_bytes(), b"PROMO") { 1 } else { 0 }))))
        .arrange_by_self()
        .join_core(&lineitems, |&_part_key, _, _| Some(()))
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
            if create_date(1995,9,1) <= l.ship_date && l.ship_date < create_date(1995,10,1) {
                Some((l.part_key, (l.extended_price * (100 - l.discount) / 100) as isize ))
            }
            else { None }
        )
        .arrange_by_self()
        .join_core(&arrangements.part, |_pk,&(),p| Some(DiffPair::new(1, if starts_with(&p.typ.as_bytes(), b"PROMO") { 1 } else { 0 })))
        .explode(|dp| Some(((),dp)))
        .count_total()
        .probe_with(probe);
}