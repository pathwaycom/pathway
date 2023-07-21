use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Discounted Revenue Query (Q19)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     sum(l_extendedprice* (1 - l_discount)) as revenue
// from
//     lineitem,
//     part
// where
//     (
//         p_partkey = l_partkey
//         and p_brand = ':1'
//         and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
//         and l_quantity >= :4 and l_quantity <= :4 + 10
//         and p_size between 1 and 5
//         and l_shipmode in ('AIR', 'AIR REG')
//         and l_shipinstruct = 'DELIVER IN PERSON'
//     )
//     or
//     (
//         p_partkey = l_partkey
//         and p_brand = ':2'
//         and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
//         and l_quantity >= :5 and l_quantity <= :5 + 10
//         and p_size between 1 and 10
//         and l_shipmode in ('AIR', 'AIR REG')
//         and l_shipinstruct = 'DELIVER IN PERSON'
//     )
//     or
//     (
//         p_partkey = l_partkey
//         and p_brand = ':3'
//         and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
//         and l_quantity >= :6 and l_quantity <= :6 + 10
//         and p_size between 1 and 15
//         and l_shipmode in ('AIR', 'AIR REG')
//         and l_shipinstruct = 'DELIVER IN PERSON'
//     );
// :n -1


fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let lineitems =
    collections
        .lineitems()
        .explode(|x|
            if (starts_with(&x.ship_mode, b"AIR") || starts_with(&x.ship_mode, b"AIR REG")) && starts_with(&x.ship_instruct, b"DELIVER IN PERSON") {
                Some(((x.part_key, x.quantity), (x.extended_price * (100 - x.discount) / 100) as isize))
            }
            else { None }
        );

    let lines1 = lineitems.filter(|&(_, quant)| quant >= 1 && quant <= 11).map(|x| x.0).arrange_by_self();
    let lines2 = lineitems.filter(|&(_, quant)| quant >= 10 && quant <= 20).map(|x| x.0).arrange_by_self();
    let lines3 = lineitems.filter(|&(_, quant)| quant >= 20 && quant <= 30).map(|x| x.0).arrange_by_self();

    let parts = collections.parts().map(|p| (p.part_key, (p.brand, p.container, p.size)));

    let parts1 = parts.filter(|&(_key, (brand, container, size))| starts_with(&brand, b"Brand#12") && 1 <= size && size <= 5 &&  (starts_with(&container, b"SM CASE") || starts_with(&container, b"SM BOX") || starts_with(&container, b"SM PACK") || starts_with(&container, b"MED PKG"))).map(|x| x.0).arrange_by_self();
    let parts2 = parts.filter(|&(_key, (brand, container, size))| starts_with(&brand, b"Brand#23") && 1 <= size && size <= 10 && (starts_with(&container, b"MED BAG") || starts_with(&container, b"MED BOX") || starts_with(&container, b"MED PKG") || starts_with(&container, b"MED PACK"))).map(|x| x.0).arrange_by_self();
    let parts3 = parts.filter(|&(_key, (brand, container, size))| starts_with(&brand, b"Brand#34") && 1 <= size && size <= 15 && (starts_with(&container, b"LG CASE") || starts_with(&container, b"LG BOX") || starts_with(&container, b"LG PACK") || starts_with(&container, b"LG PKG"))).map(|x| x.0).arrange_by_self();

    let result1 = lines1.join_core(&parts1, |_,_,_| Some(()));
    let result2 = lines2.join_core(&parts2, |_,_,_| Some(()));
    let result3 = lines3.join_core(&parts3, |_,_,_| Some(()));

    result1
        .concat(&result2)
        .concat(&result3)
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
        .explode(|x|
            if (starts_with(&x.ship_mode, b"AIR") || starts_with(&x.ship_mode, b"AIR REG")) && starts_with(&x.ship_instruct, b"DELIVER IN PERSON") {
                Some(((x.part_key, x.quantity), (x.extended_price * (100 - x.discount) / 100) as isize))
            }
            else { None }
        )
        .join_core(&arrangements.part, |_pk,&qu,p| {
            if qu >= 1  && qu <= 11 && (starts_with(&p.brand, b"Brand#12") && 1 <= p.size && p.size <= 5  && (starts_with(&p.container, b"SM CASE") || starts_with(&p.container, b"SM BOX") || starts_with(&p.container, b"SM PACK") || starts_with(&p.container, b"MED PKG")))
            && qu >= 10 && qu <= 20 && (starts_with(&p.brand, b"Brand#23") && 1 <= p.size && p.size <= 10 && (starts_with(&p.container, b"MED BAG") || starts_with(&p.container, b"MED BOX") || starts_with(&p.container, b"MED PKG") || starts_with(&p.container, b"MED PACK")))
            && qu >= 20 && qu <= 30 && (starts_with(&p.brand, b"Brand#12") && 1 <= p.size && p.size <= 15 && (starts_with(&p.container, b"LG CASE") || starts_with(&p.container, b"LG BOX") || starts_with(&p.container, b"LG PACK") || starts_with(&p.container, b"LG PKG")))
            {
                Some(())
            }
            else {
                None
            }

        })
        .count_total()
        .probe_with(probe);
}