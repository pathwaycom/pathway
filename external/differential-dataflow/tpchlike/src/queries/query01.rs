use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::*;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::lattice::Lattice;

use {Arrangements, Experiment, Collections};

// -- $ID$
// -- TPC-H/TPC-R Pricing Summary Report Query (Q1)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     l_returnflag,
//     l_linestatus,
//     sum(l_quantity) as sum_qty,
//     sum(l_extendedprice) as sum_base_price,
//     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
//     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
//     avg(l_quantity) as avg_qty,
//     avg(l_extendedprice) as avg_price,
//     avg(l_discount) as avg_disc,
//     count(*) as count_order
// from
//     lineitem
// where
//     l_shipdate <= date '1998-12-01' - interval ':1' day (3)
// group by
//     l_returnflag,
//     l_linestatus
// order by
//     l_returnflag,
//     l_linestatus;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>, probe: &mut ProbeHandle<G::Timestamp>)
where
    G::Timestamp: Lattice+TotalOrder+Ord
{
    collections
        .lineitems()
        .explode(|item|
            if item.ship_date <= ::types::create_date(1998, 9, 2) {
                Some(((item.return_flag[0], item.line_status[0]),
                    DiffPair::new(item.quantity as isize,
                    DiffPair::new(item.extended_price as isize,
                    DiffPair::new((item.extended_price * (100 - item.discount) / 100) as isize,
                    DiffPair::new((item.extended_price * (100 - item.discount) * (100 + item.tax) / 10000) as isize,
                    DiffPair::new(item.discount as isize, 1)))))))
            }
            else {
                None
            }
        )
        .count_total()
        // .inspect(|x| println!("{:?}", x))
        .probe_with(probe);
}

pub fn query_arranged<G: Scope<Timestamp=usize>>(
    scope: &mut G,
    probe: &mut ProbeHandle<usize>,
    experiment: &mut Experiment,
    _arrangements: &mut Arrangements,
)
where
    G::Timestamp: Lattice+TotalOrder+Ord
{
    experiment
        .lineitem(scope)
        .explode(|item|
            if item.ship_date <= ::types::create_date(1998, 9, 2) {
                Some(((item.return_flag[0], item.line_status[0]),
                    DiffPair::new(item.quantity as isize,
                    DiffPair::new(item.extended_price as isize,
                    DiffPair::new((item.extended_price * (100 - item.discount) / 100) as isize,
                    DiffPair::new((item.extended_price * (100 - item.discount) * (100 + item.tax) / 10000) as isize,
                    DiffPair::new(item.discount as isize, 1)))))))
            }
            else {
                None
            }
        )
        .count_total()
        .probe_with(probe);
}