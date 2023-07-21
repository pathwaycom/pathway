use timely::Data;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::channels::pushers::Tee;

use rdkafka::Message;
use rdkafka::consumer::{ConsumerContext, BaseConsumer};

/// Constructs a stream of data from a Kafka consumer.
///
/// This method assembles a stream of data from a Kafka consumer and supplied
/// user logic for determining how to interpret the binary data Kafka supplies.
///
/// The user logic is provided binary data as `&[u8]`, and mutable references to
/// a capability and an output handle, which the logic should use to produce data
/// if it is so inclined. The logic must return a bool indicating whether the stream
/// is complete (true indicates that the operator should cease data production and
/// shut down).
///
/// # Examples
/// ```rust,no_run
/// use timely::dataflow::operators::Inspect;
///
/// use rdkafka::Message;
/// use rdkafka::config::ClientConfig;
/// use rdkafka::consumer::{Consumer, BaseConsumer, DefaultConsumerContext};
///
/// fn main() {
///
///     let mut args = ::std::env::args();
///     args.next();
///
///     // Extract Kafka topic.
///     let topic = args.next().expect("Must specify a Kafka topic");
///     let brokers = "localhost:9092";
///
///     // Create Kafka consumer configuration.
///     // Feel free to change parameters here.
///     let mut consumer_config = ClientConfig::new();
///     consumer_config
///         .set("produce.offset.report", "true")
///         .set("auto.offset.reset", "smallest")
///         .set("group.id", "example")
///         .set("enable.auto.commit", "false")
///         .set("enable.partition.eof", "false")
///         .set("auto.offset.reset", "earliest")
///         .set("session.timeout.ms", "6000")
///         .set("bootstrap.servers", &brokers);
///
///     timely::execute_from_args(args, move |worker| {
///
///         // A dataflow for producing spans.
///         worker.dataflow::<u64,_,_>(|scope| {
///
///             // Create a Kafka consumer.
///             let consumer : BaseConsumer<DefaultConsumerContext> = consumer_config.create().expect("Couldn't create consumer");
///             consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");
///
///             let strings =
///             kafkaesque::source(scope, "KafkaStringSource", consumer, |bytes, capability, output| {
///
///                 // If the bytes are utf8, convert to string and send.
///                 if let Ok(text) = std::str::from_utf8(bytes) {
///                     output
///                         .session(capability)
///                         .give(text.to_string());
///                 }
///
///                 // We need some rule to advance timestamps ...
///                 let time = *capability.time();
///                 capability.downgrade(&(time + 1));
///
///                 // Indicate that we are not yet done.
///                 false
///             });
///
///             strings.inspect(|x| println!("Observed: {:?}", x));
///
///         });
///
///     }).expect("Timely computation failed somehow");
///
///     println!("Hello, world!");
/// }
/// ```
pub fn kafka_source<C, G, D, L>(
    scope: &G,
    name: &str,
    consumer: BaseConsumer<C>,
    logic: L
) -> Stream<G, D>
where
    C: ConsumerContext+'static,
    G: Scope,
    D: Data,
    L: Fn(&[u8],
          &mut Capability<G::Timestamp>,
          &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>) -> bool+'static,
{
    use timely::dataflow::operators::generic::source;
    source(scope, name, move |capability, info| {

        let activator = scope.activator_for(&info.address[..]);
        let mut cap = Some(capability);

        // define a closure to call repeatedly.
        move |output| {

            // Act only if we retain the capability to send data.
            let mut complete = false;
            if let Some(mut capability) = cap.as_mut() {

                // Indicate that we should run again.
                activator.activate();

                // Repeatedly interrogate Kafka for [u8] messages.
                // Cease only when Kafka stops returning new data.
                // Could cease earlier, if we had a better policy.
                while let Some(result) = consumer.poll(std::time::Duration::from_millis(0)) {
                    // If valid data back from Kafka
                    if let Ok(message) = result {
                        // Attempt to interpret bytes as utf8  ...
                        if let Some(payload) = message.payload() {
                            complete = logic(payload, &mut capability, output) || complete;
                        }
                    }
                    else {
                        println!("Kafka error");
                    }
                }
            }

            if complete { cap = None; }
        }

    })
}