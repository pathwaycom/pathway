extern crate rand;
extern crate timely;
extern crate differential_dataflow;

extern crate serde;
extern crate rdkafka;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let topic = std::env::args().nth(4).unwrap();

    let write = std::env::args().any(|x| x == "write");
    let read = std::env::args().any(|x| x == "read");

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut roots, mut graph, _write_token, _read_token) = worker.dataflow(|scope| {

            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            let result = bfs(&graph, &roots);

            let result =
            result.map(|(_,l)| l)
                  .consolidate()
                  .probe_with(&mut probe);

            let write_token = if write {
                Some(kafka::create_sink(&result.inner, "localhost:9092", &topic))
            } else { None };

            let read_token = if read {
                let (read_token, stream) = kafka::create_source(result.scope(), "localhost:9092", &topic, "group");
                use differential_dataflow::AsCollection;
                stream
                    .as_collection()
                    .negate()
                    .concat(&result)
                    .consolidate()
                    .inspect(|x| println!("In error: {:?}", x))
                    .probe_with(&mut probe)
                    .assert_empty()
                    ;
                Some(read_token)
            } else { None };

            (root_input, edge_input, write_token, read_token)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        roots.insert(0);
        roots.close();

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }
        }

        println!("{:?}\tloaded", timer.elapsed());

        graph.advance_to(1);
        graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));

        println!("{:?}\tstable", timer.elapsed());

        for round in 0 .. {
            if write {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            for element in 0 .. batch {
                if worker.index() == 0 {
                    graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    graph.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                }
                graph.advance_to(2 + round * batch + element);
            }
            graph.flush();

            let timer2 = ::std::time::Instant::now();
            worker.step_while(|| probe.less_than(&graph.time()));

            if worker.index() == 0 {
                let elapsed = timer2.elapsed();
                println!("{:?}\t{:?}:\t{}", timer.elapsed(), round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1)))
     })
}


pub mod kafka {

    use serde::{Serialize, Deserialize};
    use timely::scheduling::SyncActivator;
    use rdkafka::{ClientContext, config::ClientConfig};
    use rdkafka::consumer::{BaseConsumer, ConsumerContext};
    use rdkafka::error::{KafkaError, RDKafkaError};
    use differential_dataflow::capture::Writer;

    use std::hash::Hash;
    use timely::progress::Timestamp;
    use timely::dataflow::{Scope, Stream};
    use differential_dataflow::ExchangeData;
    use differential_dataflow::lattice::Lattice;

    /// Creates a Kafka source from supplied configuration information.
    pub fn create_source<G, D, T, R>(scope: G, addr: &str, topic: &str, group: &str) -> (Box<dyn std::any::Any + Send + Sync>, Stream<G, (D, T, R)>)
    where
        G: Scope<Timestamp = T>,
        D: ExchangeData + Hash + for<'a> serde::Deserialize<'a>,
        T: ExchangeData + Hash + for<'a> serde::Deserialize<'a> + Timestamp + Lattice,
        R: ExchangeData + Hash + for<'a> serde::Deserialize<'a>,
    {
        differential_dataflow::capture::source::build(scope, |activator| {
            let source = KafkaSource::new(addr, topic, group, activator);
            differential_dataflow::capture::YieldingIter::new_from(Iter::<D,T,R>::new_from(source), std::time::Duration::from_millis(10))
        })
    }

    pub fn create_sink<G, D, T, R>(stream: &Stream<G, (D, T, R)>, addr: &str, topic: &str) -> Box<dyn std::any::Any>
    where
        G: Scope<Timestamp = T>,
        D: ExchangeData + Hash + Serialize + for<'a> Deserialize<'a>,
        T: ExchangeData + Hash + Serialize + for<'a> Deserialize<'a> + Timestamp + Lattice,
        R: ExchangeData + Hash + Serialize + for<'a> Deserialize<'a>,
    {
        use std::rc::Rc;
        use std::cell::RefCell;
        use differential_dataflow::hashable::Hashable;

        let sink = KafkaSink::new(addr, topic);
        let result = Rc::new(RefCell::new(sink));
        let sink_hash = (addr.to_string(), topic.to_string()).hashed();
        differential_dataflow::capture::sink::build(
            &stream,
            sink_hash,
            Rc::downgrade(&result),
            Rc::downgrade(&result),
        );
        Box::new(result)

    }

    pub struct KafkaSource {
        consumer: BaseConsumer<ActivationConsumerContext>,
    }

    impl KafkaSource {
        pub fn new(addr: &str, topic: &str, group: &str, activator: SyncActivator) -> Self {
            let mut kafka_config = ClientConfig::new();
            // This is mostly cargo-cult'd in from `source/kafka.rs`.
            kafka_config.set("bootstrap.servers", &addr.to_string());
            kafka_config
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest");

            kafka_config.set("topic.metadata.refresh.interval.ms", "30000"); // 30 seconds
            kafka_config.set("fetch.message.max.bytes", "134217728");
            kafka_config.set("group.id", group);
            kafka_config.set("isolation.level", "read_committed");
            let activator = ActivationConsumerContext(activator);
            let consumer = kafka_config.create_with_context::<_, BaseConsumer<_>>(activator).unwrap();
            use rdkafka::consumer::Consumer;
            consumer.subscribe(&[topic]).unwrap();
            Self {
                consumer,
            }
        }
    }

    pub struct Iter<D, T, R> {
        pub source: KafkaSource,
        phantom: std::marker::PhantomData<(D, T, R)>,
    }

    impl<D, T, R> Iter<D, T, R> {
        /// Constructs a new iterator from a bytes source.
        pub fn new_from(source: KafkaSource) -> Self {
            Self {
                source,
                phantom: std::marker::PhantomData,
            }
        }
    }

    impl<D, T, R> Iterator for Iter<D, T, R>
    where
        D: for<'a>Deserialize<'a>,
        T: for<'a>Deserialize<'a>,
        R: for<'a>Deserialize<'a>,
    {
        type Item = differential_dataflow::capture::Message<D, T, R>;
        fn next(&mut self) -> Option<Self::Item> {
            use rdkafka::message::Message;
            self.source
                .consumer
                .poll(std::time::Duration::from_millis(0))
                .and_then(|result| result.ok())
                .and_then(|message| {
                    message.payload().and_then(|message| bincode::deserialize::<differential_dataflow::capture::Message<D, T, R>>(message).ok())
                })
        }
    }

    /// An implementation of [`ConsumerContext`] that unparks the wrapped thread
    /// when the message queue switches from nonempty to empty.
    struct ActivationConsumerContext(SyncActivator);

    impl ClientContext for ActivationConsumerContext { }

    impl ActivationConsumerContext {
        fn activate(&self) {
            self.0.activate().unwrap();
        }
    }

    impl ConsumerContext for ActivationConsumerContext {
        fn message_queue_nonempty_callback(&self) {
            self.activate();
        }
    }

    use std::time::Duration;
    use rdkafka::producer::DefaultProducerContext;
    use rdkafka::producer::{BaseRecord, ThreadedProducer};

    pub struct KafkaSink {
        topic: String,
        producer: ThreadedProducer<DefaultProducerContext>,
        buffer: Vec<u8>,
    }

    impl KafkaSink {
        pub fn new(addr: &str, topic: &str) -> Self {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", &addr);
            config.set("queue.buffering.max.kbytes", &format!("{}", 16 << 20));
            config.set("queue.buffering.max.messages", &format!("{}", 10_000_000));
            config.set("queue.buffering.max.ms", &format!("{}", 10));
            let producer = config
                .create_with_context::<_, ThreadedProducer<_>>(DefaultProducerContext)
                .expect("creating kafka producer for kafka sinks failed");
            Self {
                producer,
                topic: topic.to_string(),
                buffer: Vec::new(),
            }
        }
    }

    impl<T: Serialize> Writer<T> for KafkaSink {
        fn poll(&mut self, item: &T) -> Option<Duration> {
            self.buffer.clear();
            bincode::serialize_into(&mut self.buffer, item).expect("Writing to a `Vec<u8>` cannot fail");
            let record = BaseRecord::<[u8], _>::to(&self.topic).payload(&self.buffer);
            self.producer.send(record).err().map(|(e, _)| {
                if let KafkaError::MessageProduction(RDKafkaError::QueueFull) = e {
                    Duration::from_secs(1)
                } else {
                    // TODO(frank): report this error upwards so the user knows the sink is dead.
                    Duration::from_secs(1)
                }
            })
        }
        fn done(&self) -> bool {
            self.producer.in_flight_count() == 0
        }
    }

}
