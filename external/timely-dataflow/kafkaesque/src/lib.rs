use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

use abomonation::Abomonation;
use timely::dataflow::operators::capture::event::{EventCore, EventPusherCore, EventIteratorCore};

use rdkafka::Message;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, ProducerContext, DeliveryResult};
use rdkafka::consumer::{Consumer, BaseConsumer, DefaultConsumerContext};

use rdkafka::config::FromClientConfigAndContext;

pub mod kafka_source;
pub use kafka_source::kafka_source as source;

struct OutstandingCounterContext {
    outstanding: Arc<AtomicIsize>,
}

impl ClientContext for OutstandingCounterContext { }

impl ProducerContext for OutstandingCounterContext {
    type DeliveryOpaque = ();
    fn delivery(&self, _report: &DeliveryResult, _: Self::DeliveryOpaque) {
        self.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
}

impl OutstandingCounterContext {
    pub fn new(counter: &Arc<AtomicIsize>) -> Self {
        OutstandingCounterContext {
            outstanding: counter.clone()
        }
    }
}

/// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
pub struct EventProducerCore<T, D> {
    topic: String,
    buffer: Vec<u8>,
    producer: BaseProducer<OutstandingCounterContext>,
    counter: Arc<AtomicIsize>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

/// [EventProducerCore] specialized to vector-based containers.
pub type EventProducer<T, D> = EventProducerCore<T, Vec<D>>;

impl<T, D> EventProducerCore<T, D> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(config: ClientConfig, topic: String) -> Self {
        let counter = Arc::new(AtomicIsize::new(0));
        let context = OutstandingCounterContext::new(&counter);
        let producer = BaseProducer::<OutstandingCounterContext>::from_config_and_context(&config, context).expect("Couldn't create producer");
        println!("allocating producer for topic {:?}", topic);
        Self {
            topic: topic,
            buffer: vec![],
            producer: producer,
            counter: counter,
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<T: Abomonation, D: Abomonation> EventPusherCore<T, D> for EventProducerCore<T, D> {
    fn push(&mut self, event: EventCore<T, D>) {
        unsafe { ::abomonation::encode(&event, &mut self.buffer).expect("Encode failure"); }
        // println!("sending {:?} bytes", self.buffer.len());
        self.producer.send::<(),[u8]>(BaseRecord::to(self.topic.as_str()).payload(&self.buffer[..])).unwrap();
        self.counter.fetch_add(1, Ordering::SeqCst);
        self.producer.poll(std::time::Duration::from_millis(0));
        self.buffer.clear();
    }
}

impl<T, D> Drop for EventProducerCore<T, D> {
    fn drop(&mut self) {
        while self.counter.load(Ordering::SeqCst) > 0 {
            self.producer.poll(std::time::Duration::from_millis(10));
        }
    }
}

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct EventConsumerCore<T, D> {
    consumer: BaseConsumer<DefaultConsumerContext>,
    buffer: Vec<u8>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

/// [EventConsumerCore] specialized to vector-based containers.
pub type EventConsumer<T, D> = EventConsumerCore<T, Vec<D>>;

impl<T, D> EventConsumerCore<T, D> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(config: ClientConfig, topic: String) -> Self {
        println!("allocating consumer for topic {:?}", topic);
        let consumer : BaseConsumer<DefaultConsumerContext> = config.create().expect("Couldn't create consumer");
        consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");
        Self {
            consumer: consumer,
            buffer: Vec::new(),
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<T: Abomonation, D: Abomonation> EventIteratorCore<T, D> for EventConsumerCore<T, D> {
    fn next(&mut self) -> Option<&EventCore<T, D>> {
        if let Some(result) = self.consumer.poll(std::time::Duration::from_millis(0)) {
            match result {
                Ok(message) =>  {
                    self.buffer.clear();
                    self.buffer.extend_from_slice(message.payload().unwrap());
                    Some(unsafe { ::abomonation::decode::<EventCore<T,D>>(&mut self.buffer[..]).unwrap().0 })
                },
                Err(err) => {
                    println!("KafkaConsumer error: {:?}", err);
                    None
                },
            }
        }
        else { None }
    }
}
