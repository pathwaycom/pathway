use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Local Supplier Volume Query (Q5)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     n_name,
//     sum(l_extendedprice * (1 - l_discount)) as revenue
// from
//     customer,
//     orders,
//     lineitem,
//     supplier,
//     nation,
//     region
// where
//     c_custkey = o_custkey
//     and l_orderkey = o_orderkey
//     and l_suppkey = s_suppkey
//     and c_nationkey = s_nationkey
//     and s_nationkey = n_nationkey
//     and n_regionkey = r_regionkey
//     and r_name = ':1'
//     and o_orderdate >= date ':2'
//     and o_orderdate < date ':2' + interval '1' year
// group by
//     n_name
// order by
//     revenue desc;
// :n -1

fn starts_with(source: &[u8], query: &[u8]) -> bool {
    source.len() >= query.len() && &source[..query.len()] == query
}

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where G::Timestamp: Lattice+TotalOrder+Ord {

    let regions =
    collections
        .regions()
        .filter(|x| starts_with(&x.name[..], b"ASIA"))
        .map(|x| x.region_key);

    let nations =
    collections
        .nations()
        .map(|x| (x.region_key, x.nation_key))
        .semijoin(&regions)
        .map(|(_region_key, nation_key)| nation_key);

    let suppliers =
    collections
        .suppliers()
        .map(|x| (x.nation_key, x.supp_key))
        .semijoin(&nations)
        .map(|(_nat, supp)| (supp, _nat));

    let customers =
    collections
        .customers()
        .map(|c| (c.nation_key, c.cust_key))
        .semijoin(&nations)
        .map(|c| (c.1, c.0));

    let orders =
    collections
        .orders()
        .flat_map(|o|
            if o.order_date >= create_date(1994, 1, 1) && o.order_date < create_date(1995, 1, 1) {
                Some((o.cust_key, o.order_key))
            }
            else { None }
        )
        .join(&customers)
        .map(|o| o.1);

    let lineitems = collections
        .lineitems()
        .explode(|l| Some(((l.order_key, l.supp_key), (l.extended_price * (100 - l.discount) / 100) as isize)))
        .join(&orders)
        .map(|(_order, (supp, nat))| (supp, nat));

    suppliers
        .map(|x| (x, ()))
        .semijoin(&lineitems)
        .map(|((_supp, nat), ())| nat)
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
        .explode(|l| Some(((l.order_key, l.supp_key), (l.extended_price * (100 - l.discount) / 100) as isize)))
        .join_core(&arrangements.order, |_ok, &sk, o| {
            if o.order_date >= create_date(1994, 1, 1) && o.order_date < create_date(1995, 1, 1) {
                Some((o.cust_key, sk))
            }
            else { None }
        })
        .join_core(&arrangements.customer, |_ck, &sk, c| Some((sk,c.nation_key)))
        .join_core(&arrangements.supplier, |_sk, &nk, s| {
            if s.nation_key == nk {
                Some((nk, ()))
            }
            else { None }
        })
        .join_core(&arrangements.nation, |_nk, &(), n| Some((n.region_key, n.name)))
        .join_core(&arrangements.region, |_rk, &name, r| {
            if starts_with(&r.name[..], b"ASIA") { Some(name) } else { None }
        })
        .count_total()
        .probe_with(probe);
}