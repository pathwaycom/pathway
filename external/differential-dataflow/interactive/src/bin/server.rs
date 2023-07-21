extern crate timely;
extern crate differential_dataflow;
extern crate interactive;

use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;
use std::thread::Thread;

use timely::synchronization::Sequencer;
use interactive::{Command, Manager};
use interactive::concrete::Value;

fn main() {

    let mut args = std::env::args();
    args.next();

    let (root_send, root_recv) = std::sync::mpsc::channel::<(Sender<Command<Value>>, Thread)>();
    let root_send = Arc::new(Mutex::new(root_send));

    std::thread::Builder::new()
        .name("Listener".to_string())
        .spawn(move || {

            let (send, thread) = root_recv.recv().expect("Did not receive channel to worker");

            use std::net::TcpListener;
            let listener = TcpListener::bind("127.0.0.1:8000".to_string()).expect("failed to bind listener");
            for mut stream in listener.incoming() {
                let mut stream = stream.expect("listener error");
                let send = send.clone();
                let thread = thread.clone();
                std::thread::Builder::new()
                    .name("Client".to_string())
                    .spawn(move || {
                        while let Ok(command) = bincode::deserialize_from::<_,Command<Value>>(&mut stream) {
                            send.send(command).expect("command send failed");
                            thread.unpark();
                        }
                    })
                    .expect("failed to create thread");
            }
        })
        .expect("Failed to spawn listen thread");

    // Initiate timely computation.
    timely::execute_from_args(args, move |worker| {

        // Send an endpoint and thread handle to root.
        let (send, recv) = std::sync::mpsc::channel();
        root_send
            .lock()
            .expect("lock poisoned")
            .send((send, std::thread::current()))
            .expect("send failed");

        let timer = ::std::time::Instant::now();

        let mut manager = Manager::<Value>::new();
        let mut sequencer: Option<Sequencer<Command<Value>>> = Some(Sequencer::new(worker, timer));

        while sequencer.is_some() {

            // Check out channel status.
            while let Ok(command) = recv.try_recv() {
                sequencer
                    .as_mut()
                    .map(|s| s.push(command));
            }

            // Dequeue and act on commands.
            // Once per iteration, so that Shutdown works "immediately".
            if let Some(command) = sequencer.as_mut().and_then(|s| s.next()) {
                if command == Command::Shutdown {
                    sequencer = None;
                }
                command.execute(&mut manager, worker);
                worker.step();
            }
            else {
                // Only consider parking if the sequencer is empty too.
                worker.step_or_park(None);
            }
        }

        println!("Shutting down");

    }).expect("Timely computation did not initialize cleanly");
}
