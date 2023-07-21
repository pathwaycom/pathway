extern crate libloading;
extern crate timely;
extern crate dd_server;

use std::io::BufRead;
use std::io::Write;

use std::sync::{Arc, Mutex};

use timely::synchronization::Sequencer;

use libloading::{Library, Symbol};

use dd_server::{Environment, TraceHandler};

fn main() {

    // shared queue of commands to serialize (in the "put in an order" sense).
    let (send, recv) = std::sync::mpsc::channel::<Vec<String>>();
    let recv = Arc::new(Mutex::new(recv));

    // demonstrate dynamic loading of dataflows via shared libraries.
    let guards = timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();
        let recv = recv.clone();

        // map from string name to arranged graph.
        let mut handles = TraceHandler::new();

        // common probe used by all dataflows to express progress information.
        let mut probe = timely::dataflow::operators::probe::Handle::new();

        // queue shared between serializer (producer) and command loop (consumer).
        let mut sequencer = Sequencer::new(worker, timer);

        let mut done = false;
        while !done {

            // Check out channel status.
            let lock = recv.lock().expect("Mutex poisoned");
            use std::sync::mpsc::TryRecvError;
            match lock.try_recv() {
                Ok(command) => { sequencer.push(command); },
                Err(TryRecvError::Empty) => { },
                Err(TryRecvError::Disconnected) => { done = true; },
            };

            // Dequeue and act on commands.
            while let Some(mut command) = sequencer.next() {

                let index = worker.index();
                println!("worker {:?}: received command: {:?}", index, command);

                if command.len() > 1 {
                    let operation = command.remove(0);
                    match operation.as_str() {
                        "list" => {
                            println!("worker {:?} listing", index);
                            for key in handles.keys() {
                                println!("worker {:?} list: {:?}", index, key);
                            }
                        }
                        "load" => {

                            if command.len() >= 2 {

                                let library_path = &command[0];
                                let symbol_name = &command[1];

                                if let Ok(lib) = Library::new(library_path) {
                                    worker.dataflow_core("dataflow", None, lib, |lib, child| {
                                        let result = unsafe {
                                            lib.get::<Symbol<unsafe fn(Environment)->Result<(),String>>>(symbol_name.as_bytes())
                                            .map(|func| func((child, &mut handles, &mut probe, &timer, &command[2..])))
                                        };

                                        match result {
                                            Err(_) => { println!("worker {:?}: failed to find symbol {:?} in shared library {:?}.", index, symbol_name, library_path); },
                                            Ok(Err(x)) => { println!("worker {:?}: error: {:?}", index, x); },
                                            Ok(Ok(())) => { /* Good news, everyone! */ },
                                        }
                                    });
                                }
                                else {
                                    println!("worker {:?}: failed to open shared library: {:?}", index, library_path);
                                }
                            }
                        },
                        "drop" => {
                            for name in command.iter() {
                                handles.remove(name);
                            }
                        }
                        _ => {
                            println!("worker {:?}: unrecognized command: {:?}", index, operation);
                        }
                    }
                }

                // arguably we should pick a time (now) and `step_while` until it has passed.
                // this should ensure that we actually fully drain ranges of updates, rather
                // than providing no guaranteed progress for e.g. iterative computations.

                worker.step();
            }
        }

        println!("worker {}: command queue unavailable; exiting command loop.", worker.index());
    });

    // the main thread now continues, to read from the console and issue work to the shared queue.

    std::io::stdout().flush().unwrap();
    let input = std::io::stdin();

    let mut done = false;

    while !done {

        if let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
            let elts: Vec<_> = line.split_whitespace().map(|x| x.to_owned()).collect();

            if elts.len() > 0 {
                match elts[0].as_str() {
                    "help" => { println!("valid commands are currently: bind, drop, exit, help, list, load"); },
                    "bind" => { println!("ideally this would load and bind a library to some delightful name"); },
                    "drop" => { send.send(elts).expect("failed to send command"); }
                    "exit" => { done = true; },
                    "load" => { send.send(elts).expect("failed to send command"); },
                    "list" => { send.send(elts).expect("failed to send command"); },
                    _ => { println!("unrecognized command: {:?}", elts[0]); },
                }
            }

            std::io::stdout().flush().unwrap();
        }
    }

    println!("main: exited command loop");
    drop(send);

    guards.unwrap();
}
