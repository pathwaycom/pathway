# Capture and Replay

Timely dataflow has two fairly handy operators, `capture_into` and `replay_into`, that are great for transporting a timely dataflow stream from its native representation into data, and then back again. They are also a fine way to think about interoperating with other systems for streaming data.

## Capturing Streams

At its core, `capture_into` records everything it sees about the stream it is attached to. If some data arrive, it records that. If there is a change in the possibility that timestamps might arrive on its input, it records that.

The `capture_into` method is relative simple, and we can just look at it:

```rust,ignore
    fn capture_into<P: EventPusher<S::Timestamp, D>+'static>(&self, event_pusher: P) {

        let mut builder = OperatorBuilder::new("Capture".to_owned(), self.scope());
        let mut input = PullCounter::new(builder.new_input(self, Pipeline));
        let mut started = false;

        let event_pusher1 = Rc::new(RefCell::new(event_pusher));
        let event_pusher2 = event_pusher1.clone();

        builder.build(
            move |frontier| {
                if !started {
                    frontier[0].update(Default::default(), -1);
                    started = true;
                }
                if !frontier[0].is_empty() {
                    let to_send = ::std::mem::replace(&mut frontier[0], ChangeBatch::new());
                    event_pusher1.borrow_mut().push(Event::Progress(to_send.into_inner()));
                }
            },
            move |consumed, _internal, _external| {
                let mut borrow = event_pusher2.borrow_mut();
                while let Some((time, data)) = input.next() {
                    borrow.push(Event::Messages(time.clone(), data.deref_mut().clone()));
                }
                input.consumed().borrow_mut().drain_into(&mut consumed[0]);
                false
            }
        );
    }
```

The method is generic with respect to some implementor `P` of the trait `EventPusher` which defines a method `push` that accepts `Event<T, D>` items (we will see a few implementations in just a moment). After a bit of set-up, `capture_into` builds a new operator with one input and zero outputs, and sets the logic for (i) what to do when the input frontier changes, and (ii) what to do when presented with the opportunity to do a bit of computation. In both cases, we just create new events based on what we see (progress changes and data messages, respectively).

There is a mysterious subtraction of `Default::default()`, which has to do with the contract that the replaying operators assume the stream starts with such a capability. This prevents the need for the replayers to block on the stream in their operator construction (any operator must state any initial capabilities as part of its construction; it cannot defer that until later).

One nice aspect of `capture_into` is that it really does reveal everything that an operator sees about a stream. If you got your hands on the resulting sequence of events, you would be able to review the full history of the stream. In principle, this could be a fine place to persist the data, capturing both data and progress information.

## Replaying Streams

At *its* core, `replay_into` takes some sequence of `Event<T, D>` items and reproduces the stream, as it was recorded. It is also fairly simple, and we can just look at its implementation as well:

```rust,ignore
    fn replay_into<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>{

        let mut builder = OperatorBuilder::new("Replay".to_owned(), scope.clone());
        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;

        builder.build(
            move |_frontier| { },
            move |_consumed, internal, produced| {

                if !started {
                    internal[0].update(Default::default(), (event_streams.len() as i64) - 1);
                    started = true;
                }

                for event_stream in event_streams.iter_mut() {
                    while let Some(event) = event_stream.next() {
                        match *event {
                            Event::Start => { },
                            Event::Progress(ref vec) => {
                                internal[0].extend(vec.iter().cloned());
                            },
                            Event::Messages(ref time, ref data) => {
                                output.session(time).give_iterator(data.iter().cloned());
                            }
                        }
                    }
                }

                output.cease();
                output.inner().produced().borrow_mut().drain_into(&mut produced[0]);

                false
            }
        );

        stream
   }
```

The type of `self` here is actually something that allows us to enumerate a sequence of event streams, so each replayer is actually replaying some variable number of streams. As part of this, our very first action is to amend our initial `Default::default()` capability to have multiplicity equal to the number of streams we are replaying:

```rust,ignore
                if !started {
                    internal[0].update(Default::default(), (event_streams.len() as i64) - 1);
                    started = true;
                }
```

If we have multiple streams, we'll now have multiple capabilities. If we have no stream, we will just drop the capability. This change is important because each source stream believes it has such a capability, and we will eventually see this many drops of the capability in the event stream (though perhaps not immediately; the initial deletion we inserted in `capture_into` likely cancels with the initial capabilities expressed by the outside world; we will likely need to wait until the captured stream is informed about the completion of messages with the default time).

Having done the initial adjustment, we literally just play out the streams (note the plural) as they are available. The `next` method is expected not to block, but rather to return `None` when there are no more events currently available. It is a bit of a head-scratcher, but any interleaving of these streams is itself a valid stream (messages are sent and capabilities claimed only when we hold appropriate capabilities).

## An Example

We can check out the examples `examples/capture_send.rs` and `examples/capture_recv.rs` to see a paired use of capture and receive demonstrating the generality.

The `capture_send` example creates a new TCP connection for each worker, which it wraps and uses as an `EventPusher`. Timely dataflow takes care of all the serialization and stuff like that (warning: it uses abomonation, so this is not great for long-term storage).

```rust,ignore
extern crate timely;

use std::net::TcpStream;
use timely::dataflow::operators::ToStream;
use timely::dataflow::operators::capture::{Capture, EventWriter};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let addr = format!("127.0.0.1:{}", 8000 + worker.index());
        let send = TcpStream::connect(addr).unwrap();

        worker.dataflow::<u64,_,_>(|scope|
            (0..10u64)
                .to_stream(scope)
                .capture_into(EventWriter::new(send))
        );
    }).unwrap();
}
```

The `capture_recv` example is more complicated, because we may have a different number of workers replaying the stream than initially captured it.

```rust,ignore
extern crate timely;

use std::net::TcpListener;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::capture::{EventReader, Replay};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

        // create replayers from disjoint partition of source worker identifiers.
        let replayers =
        (0 .. source_peers)
            .filter(|i| i % worker.peers() == worker.index())
            .map(|i| TcpListener::bind(format!("127.0.0.1:{}", 8000 + i)).unwrap())
            .collect::<Vec<_>>()
            .into_iter()
            .map(|l| l.incoming().next().unwrap().unwrap())
            .map(|r| EventReader::<_,u64,_>::new(r))
            .collect::<Vec<_>>();

        worker.dataflow::<u64,_,_>(|scope| {
            replayers
                .replay_into(scope)
                .inspect(|x| println!("replayed: {:?}", x));
        })
    }).unwrap(); // asserts error-free execution
}
```

Almost all of the code up above is assigning responsibility for the replaying between the workers we have (from `worker.peers()`). We partition responsibility for `0 .. source_peers` among the workers, create `TcpListener`s to handle the connection requests, wrap them in `EventReader`s, and then collect them up as a vector. The workers have collectively partitioned the incoming captured streams between themselves.

Finally, each worker just uses the list of `EventReader`s as the argument to `replay_into`, and we get the stream magically transported into a new dataflow, in a different process, with a potentially different number of workers.

If you want to try it out, make sure to start up the `capture_recv` example first (otherwise the connections will be refused for `capture_send`) and specify the expected number of source workers, modifying the number of received workers if you like. Here we are expecting five source workers, and distributing them among three receive workers (to make life complicated):

```ignore
    shell1% cargo run --example capture_recv -- 5 -w3
```

Nothing happens yet, so head over to another shell and run `capture_send` with the specified number of workers (five, in this case):

```ignore
    shell2% cargo run --example capture_send -- -w5
```

Now, back in your other shell you should see something like

```ignore
    shell1% cargo run --example capture_recv -- 5 -w3
    replayed: 0
    replayed: 1
    replayed: 2
    replayed: 3
    replayed: 4
    replayed: 5
    replayed: 0
    replayed: 6
    replayed: 1
    ...
```

which just goes on and on, but which should produce 50 lines of text, with five copies of `0 .. 10` interleaved variously.

## Capture types

There are several sorts of things you could capture into and replay from. In the `capture::events` module you will find two examples, a linked list and a binary serializer / deserializer (wrapper around `Write` and `Read` traits). The binary serializer is fairly general; we used it up above to wrap TCP streams. You could also write to files, or write to shared memory. However, be mindful that the serialization format (abomonation) is essentially the in-memory representation, and Rust makes no guarantees about the stability of such a representation across builds.

There is also [an in-progress Kafka adapter](https://github.com/TimelyDataflow/timely-dataflow/tree/master/kafkaesque) available in the repository, which uses Kafka topics to store the binary representation of captured streams, which can then be replayed by any timely computation that can read them. This may be a while before it is sorted out, because Kafka seems to have a few quirks, but if you would like to help get in touch.
