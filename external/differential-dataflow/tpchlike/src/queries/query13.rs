use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use regex::Regex;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Customer Distribution Query (Q13)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     c_count,
//     count(*) as custdist
// from
//     (
//         select
//             c_custkey,
//             count(o_orderkey)
//         from
//             customer left outer join orders on
//                 c_custkey = o_custkey
//                 and o_comment not like '%:1%:2%'
//         group by
//             c_custkey
//     ) as c_orders (c_custkey, c_count)
// group by
//     c_count
// order by
//     custdist desc,
//     c_count desc;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let regex = Regex::new("special.*requests").expect("Regex construction failed");

    let orders =
    collections
        .orders()
        .flat_map(move |o| if !regex.is_match(&o.comment) { Some(o.cust_key) } else { None } );

    collections
        .customers()
        .map(|c| c.cust_key)
        .concat(&orders)
        .count_total()
        .map(|(_cust_key, count)| (count-1) as usize)
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

    let regex = Regex::new("special.*requests").expect("Regex construction failed");

    let orders =
    arrangements
        .order
        .flat_map_ref(move |_,o| if !regex.is_match(&o.comment) { Some(o.cust_key) } else { None } )
        .inner
        .map(move |(d,t,r)| (d, ::std::cmp::max(t,round),r))
        .as_collection();

    arrangements
        .customer
        .as_collection(|&c,_| c)
        .inner
        .map(move |(d,t,r)| (d, ::std::cmp::max(t,round),r))
        .as_collection()
        .concat(&orders)
        .count_total()
        .map(|(_cust_key, count)| (count-1) as usize)
        .count_total()
        .probe_with(probe);
}