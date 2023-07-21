#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate differential_dataflow;
extern crate arrayvec;
extern crate regex;

use std::rc::Rc;

use timely::dataflow::*;
use timely::dataflow::operators::CapabilitySet;

use differential_dataflow::Collection;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ShutdownButton;

pub mod types;
pub mod queries;

pub use types::*;

pub struct InputHandles {
    pub customer: InputHandle<usize, (Customer, usize, isize)>,
    pub lineitem: InputHandle<usize, (Rc<LineItem>, usize, isize)>,
    pub nation: InputHandle<usize, (Nation, usize, isize)>,
    pub order: InputHandle<usize, (Order, usize, isize)>,
    pub part: InputHandle<usize, (Part, usize, isize)>,
    pub partsupp: InputHandle<usize, (PartSupp, usize, isize)>,
    pub region: InputHandle<usize, (Region, usize, isize)>,
    pub supplier: InputHandle<usize, (Supplier, usize, isize)>,
}

impl InputHandles {
    pub fn new() -> Self {
        Self {
            customer: InputHandle::new(),
            lineitem: InputHandle::new(),
            nation: InputHandle::new(),
            order: InputHandle::new(),
            part: InputHandle::new(),
            partsupp: InputHandle::new(),
            region: InputHandle::new(),
            supplier: InputHandle::new(),
        }
    }
    pub fn advance_to(&mut self, round: usize) {
        self.customer.advance_to(round);
        self.lineitem.advance_to(round);
        self.nation.advance_to(round);
        self.order.advance_to(round);
        self.part.advance_to(round);
        self.partsupp.advance_to(round);
        self.region.advance_to(round);
        self.supplier.advance_to(round);
    }
    pub fn close(self) {
        self.customer.close();
        self.lineitem.close();
        self.nation.close();
        self.order.close();
        self.part.close();
        self.partsupp.close();
        self.region.close();
        self.supplier.close();
    }
}

pub struct Collections<G: Scope> {
    customers: Collection<G, Customer, isize>,
    lineitems: Collection<G, Rc<LineItem>, isize>,
    nations: Collection<G, Nation, isize>,
    orders: Collection<G, Order, isize>,
    parts: Collection<G, Part, isize>,
    partsupps: Collection<G, PartSupp, isize>,
    regions: Collection<G, Region, isize>,
    suppliers: Collection<G, Supplier, isize>,
    used: [bool; 8],
}

impl<G: Scope> Collections<G> {
    pub fn new(
        customers: Collection<G, Customer, isize>,
        lineitems: Collection<G, Rc<LineItem>, isize>,
        nations: Collection<G, Nation, isize>,
        orders: Collection<G, Order, isize>,
        parts: Collection<G, Part, isize>,
        partsupps: Collection<G, PartSupp, isize>,
        regions: Collection<G, Region, isize>,
        suppliers: Collection<G, Supplier, isize>,
    ) -> Self {

        Collections {
            customers: customers,
            lineitems: lineitems,
            nations: nations,
            orders: orders,
            parts: parts,
            partsupps: partsupps,
            regions: regions,
            suppliers: suppliers,
            used: [false; 8]
        }
    }

    pub fn customers(&mut self) -> &Collection<G, Customer, isize> { self.used[0] = true; &self.customers }
    pub fn lineitems(&mut self) -> &Collection<G, Rc<LineItem>, isize> { self.used[1] = true; &self.lineitems }
    pub fn nations(&mut self) -> &Collection<G, Nation, isize> { self.used[2] = true; &self.nations }
    pub fn orders(&mut self) -> &Collection<G, Order, isize> { self.used[3] = true; &self.orders }
    pub fn parts(&mut self) -> &Collection<G, Part, isize> { self.used[4] = true; &self.parts }
    pub fn partsupps(&mut self) -> &Collection<G, PartSupp, isize> { self.used[5] = true; &self.partsupps }
    pub fn regions(&mut self) -> &Collection<G, Region, isize> { self.used[6] = true; &self.regions }
    pub fn suppliers(&mut self) -> &Collection<G, Supplier, isize> { self.used[7] = true; &self.suppliers }

    pub fn used(&self) -> [bool; 8] { self.used }
}


use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};

type ArrangedScope<G, K, T> = Arranged<G, ArrangedIndex<K, T>>;
type ArrangedIndex<K, T> = TraceAgent<DefaultValTrace<K, T, usize, isize>>;

pub struct ArrangementsInScope<G: Scope<Timestamp=usize>> {
    customer:   ArrangedScope<G, usize, Customer>,
    nation:     ArrangedScope<G, usize, Nation>,
    order:      ArrangedScope<G, usize, Order>,
    part:       ArrangedScope<G, usize, Part>,
    partsupp:   ArrangedScope<G, (usize, usize), PartSupp>,
    region:     ArrangedScope<G, usize, Region>,
    supplier:   ArrangedScope<G, usize, Supplier>,
}

pub struct Arrangements {
    arrange:    bool,
    customer:   ArrangedIndex<usize, Customer>,
    nation:     ArrangedIndex<usize, Nation>,
    order:      ArrangedIndex<usize, Order>,
    part:       ArrangedIndex<usize, Part>,
    partsupp:   ArrangedIndex<(usize, usize), PartSupp>,
    region:     ArrangedIndex<usize, Region>,
    supplier:   ArrangedIndex<usize, Supplier>,
}

use timely::dataflow::Scope;

impl Arrangements {

    pub fn new<G: Scope<Timestamp=usize>>(inputs: &mut InputHandles, scope: &mut G, probe: &mut ProbeHandle<usize>, arrange: bool) -> Self {

        use timely::dataflow::operators::Input;
        use timely::dataflow::operators::Probe;
        use differential_dataflow::AsCollection;
        use differential_dataflow::trace::TraceReader;

        let empty_frontier = timely::progress::Antichain::new();
        let empty_frontier = empty_frontier.borrow();
        let mut arranged = scope.input_from(&mut inputs.customer).as_collection().map(|x| (x.cust_key, x)).arrange_by_key();
        arranged.stream.probe_with(probe);
        arranged.trace.set_physical_compaction(empty_frontier);
        let customer = arranged.trace;

        let mut arranged = scope.input_from(&mut inputs.nation).as_collection().map(|x| (x.nation_key, x)).arrange_by_key();
        arranged.stream.probe_with(probe);
        arranged.trace.set_physical_compaction(empty_frontier);
        let nation = arranged.trace;

        let mut arranged = scope.input_from(&mut inputs.order).as_collection().map(|x| (x.order_key, x)).arrange_by_key();
        arranged.stream.probe_with(probe);
        arranged.trace.set_physical_compaction(empty_frontier);
        let order = arranged.trace;

        let mut arranged = scope.input_from(&mut inputs.part).as_collection().map(|x| (x.part_key, x)).arrange_by_key();
        arranged.stream.probe_with(probe);
        arranged.trace.set_physical_compaction(empty_frontier);
        let part = arranged.trace;

        let mut arranged = scope.input_from(&mut inputs.partsupp).as_collection().map(|x| ((x.part_key, x.supp_key), x)).arrange_by_key();
        arranged.stream.probe_with(probe);
        arranged.trace.set_physical_compaction(empty_frontier);
        let partsupp = arranged.trace;

        let mut arranged = scope.input_from(&mut inputs.region).as_collection().map(|x| (x.region_key, x)).arrange_by_key();
        arranged.stream.probe_with(probe);
        arranged.trace.set_physical_compaction(empty_frontier);
        let region = arranged.trace;

        let mut arranged = scope.input_from(&mut inputs.supplier).as_collection().map(|x| (x.supp_key, x)).arrange_by_key();
        arranged.stream.probe_with(probe);
        arranged.trace.set_physical_compaction(empty_frontier);
        let supplier = arranged.trace;

        Arrangements {
            arrange,
            customer,
            nation,
            order,
            part,
            partsupp,
            region,
            supplier,
        }
    }

    pub fn in_scope<G: Scope<Timestamp=usize>>(&mut self, scope: &mut G, experiment: &mut Experiment) -> ArrangementsInScope<G> {
        let (mut customer, button) = self.customer.import_core(scope, "customer");
        if !self.arrange { customer = customer.as_collection(|&k,v| (k,v.clone())).arrange_by_key(); }
        experiment.buttons.push(button);
        let (mut nation, button) = self.nation.import_core(scope, "nation");
        if !self.arrange { nation = nation.as_collection(|&k,v| (k,v.clone())).arrange_by_key(); }
        experiment.buttons.push(button);
        let (mut order, button) = self.order.import_core(scope, "order");
        if !self.arrange { order = order.as_collection(|&k,v| (k,v.clone())).arrange_by_key(); }
        experiment.buttons.push(button);
        let (mut part, button) = self.part.import_core(scope, "part");
        if !self.arrange { part = part.as_collection(|&k,v| (k,v.clone())).arrange_by_key(); }
        experiment.buttons.push(button);
        let (mut partsupp, button) = self.partsupp.import_core(scope, "partsupp");
        if !self.arrange { partsupp = partsupp.as_collection(|&k,v| (k,v.clone())).arrange_by_key(); }
        experiment.buttons.push(button);
        let (mut region, button) = self.region.import_core(scope, "region");
        if !self.arrange { region = region.as_collection(|&k,v| (k,v.clone())).arrange_by_key(); }
        experiment.buttons.push(button);
        let (mut supplier, button) = self.supplier.import_core(scope, "supplier");
        if !self.arrange { supplier = supplier.as_collection(|&k,v| (k,v.clone())).arrange_by_key(); }
        experiment.buttons.push(button);

        ArrangementsInScope {
            customer,
            nation,
            order,
            part,
            partsupp,
            region,
            supplier,
        }
    }

    pub fn set_logical_compaction(&mut self, frontier: &[usize]) {

        use differential_dataflow::trace::TraceReader;
        use timely::progress::frontier::AntichainRef;
        let frontier = AntichainRef::new(frontier);
        self.customer.set_logical_compaction(frontier);
        self.nation.set_logical_compaction(frontier);
        self.order.set_logical_compaction(frontier);
        self.part.set_logical_compaction(frontier);
        self.partsupp.set_logical_compaction(frontier);
        self.region.set_logical_compaction(frontier);
        self.supplier.set_logical_compaction(frontier);
    }
}

use std::rc::Weak;
pub struct Experiment {
    pub index: usize,
    pub token: Weak<()>,
    pub lineitem: InputHandle<usize, (Rc<LineItem>, usize, isize)>,
    pub buttons: Vec<ShutdownButton<CapabilitySet<usize>>>,
}

impl Experiment {
    pub fn new(index: usize, token: &Rc<()>) -> Self {
        Self {
            index,
            token: std::rc::Rc::downgrade(token),
            lineitem: InputHandle::new(),
            buttons: Vec::new(),
        }
    }
    pub fn lineitem<G: Scope<Timestamp=usize>>(&mut self, scope: &mut G) -> Collection<G, Rc<LineItem>, isize> {
        use timely::dataflow::operators::Input;
        use differential_dataflow::AsCollection;
        scope.input_from(&mut self.lineitem).as_collection()
    }
    pub fn close(mut self) -> Weak<()> {
        self.lineitem.close();
        for mut button in self.buttons.drain(..) { button.press(); }
        self.token
    }
}