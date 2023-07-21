use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Top Supplier Query (Q15)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// create view revenue:s (supplier_no, total_revenue) as
//     select
//         l_suppkey,
//         sum(l_extendedprice * (1 - l_discount))
//     from
//         lineitem
//     where
//         l_shipdate >= date ':1'
//         and l_shipdate < date ':1' + interval '3' month
//     group by
//         l_suppkey;
//
// :o
// select
//     s_suppkey,
//     s_name,
//     s_address,
//     s_phone,
//     total_revenue
// from
//     supplier,
//     revenue:s
// where
//     s_suppkey = supplier_no
//     and total_revenue = (
//         select
//             max(total_revenue)
//         from
//             revenue:s
//     )
// order by
//     s_suppkey;
//
// drop view revenue:s;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    // revenue by supplier
    let revenue =
        collections
            .lineitems()
            .explode(|item|
                if create_date(1996, 1, 1) <= item.ship_date && item.ship_date < create_date(1996,4,1) {
                    Some((item.supp_key, (item.extended_price * (100 - item.discount) / 100) as isize))
                }
                else { None }
            );

    // suppliers with maximum revenue
    let top_suppliers =
        revenue
            // do a hierarchical min, to improve update perf.
            .map(|key| ((key % 1000) as u16, key))
            .reduce(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
            })
            .map(|(_,key)| ((key % 100) as u8, key))
            .reduce(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
            })
            .map(|(_,key)| ((key % 10) as u8, key))
            .reduce(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
            })
            .map(|(_,key)| ((), key))
            .reduce(|_k, s, t| {
                let max = s.iter().map(|x| x.1).max().unwrap();
                t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
            })
            .map(|(_, key)| key)
            .count_total();

    collections
        .suppliers()
        .map(|s| (s.supp_key, (s.name, s.address, s.phone)))
        .join(&top_suppliers)
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

    // revenue by supplier
    let revenue =
    experiment
        .lineitem(scope)
        .explode(|item|
            if create_date(1996, 1, 1) <= item.ship_date && item.ship_date < create_date(1996,4,1) {
                Some((item.supp_key, (item.extended_price * (100 - item.discount) / 100) as isize))
            }
            else { None }
        );

    // suppliers with maximum revenue
    let top_suppliers =
    revenue
        // do a hierarchical min, to improve update perf.
        .map(|key| ((key % 1000) as u16, key))
        .reduce(|_k, s, t| {
            let max = s.iter().map(|x| x.1).max().unwrap();
            t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
        })
        .map(|(_,key)| ((key % 100) as u8, key))
        .reduce(|_k, s, t| {
            let max = s.iter().map(|x| x.1).max().unwrap();
            t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
        })
        .map(|(_,key)| ((key % 10) as u8, key))
        .reduce(|_k, s, t| {
            let max = s.iter().map(|x| x.1).max().unwrap();
            t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
        })
        .map(|(_,key)| ((), key))
        .reduce(|_k, s, t| {
            let max = s.iter().map(|x| x.1).max().unwrap();
            t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
        })
        .map(|(_, key)| key)
        .count_total();

    top_suppliers
        .join_core(&arrangements.supplier, |_sk,&rev,s| Some((s.supp_key, s.name, s.address, s.phone, rev)))
        .count_total()
        .probe_with(probe);
}