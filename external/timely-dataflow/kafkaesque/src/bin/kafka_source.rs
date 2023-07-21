use timely::dataflow::operators::Inspect;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer, DefaultConsumerContext};

fn main() {

    let mut args = ::std::env::args();
    args.next();

    // Extract Kafka topic.
    let topic = args.next().expect("Must specify a Kafka topic");
    let brokers = "localhost:9092";

    // Create Kafka consumer configuration.
    // Feel free to change parameters here.
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("produce.offset.report", "true")
        .set("auto.offset.reset", "smallest")
        .set("group.id", "example")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("bootstrap.servers", &brokers);

    timely::execute_from_args(args, move |worker| {

        // A dataflow for producing spans.
        worker.dataflow::<u64,_,_>(|scope| {

            // Create a Kafka consumer.
            let consumer : BaseConsumer<DefaultConsumerContext> = consumer_config.create().expect("Couldn't create consumer");
            consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");

            let strings =
            kafkaesque::source(scope, "KafkaStringSource", consumer, |bytes, capability, output| {

                // If the bytes are utf8, convert to string and send.
                if let Ok(text) = std::str::from_utf8(bytes) {
                    output
                        .session(capability)
                        .give(text.to_string());
                }

                // We need some rule to advance timestamps ...
                let time = *capability.time();
                capability.downgrade(&(time + 1));

                // Indicate that we are not yet done.
                false
            });

            strings.inspect(|x| println!("Observed: {:?}", x));

        });

    }).expect("Timely computation failed somehow");

    println!("Hello, world!");
}

