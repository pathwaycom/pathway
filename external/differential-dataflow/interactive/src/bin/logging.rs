extern crate interactive;

use interactive::{Command, Plan};
use interactive::concrete::Session;

fn main() {

    let socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");
    let mut session = Session::new(socket);

    session.issue(
    Command::SourceLogging(
        "127.0.0.1:9000".to_string(),   // port the server should listen on.
        "timely".to_string(),           // flavor of logging (of "timely", "differential").
        1,                              // number of worker connections to await.
        1_000_000_000,                  // maximum granularity in nanoseconds.
        "remote".to_string()            // name to use for publication.
    ));

    session.issue(
        Plan::source("logs/remote/timely/operates")
            .inspect("operates")
            .into_rule("operates"));

    session.issue(
        Plan::source("logs/remote/timely/shutdown")
            .inspect("shutdown")
            .into_rule("shutdown"));

    // session.issue(
    //     Plan::source("logs/remote/timely/channels")
    //         .inspect("channels")
    //         .into_rule("channels"));

    // session.issue(
    //     Plan::source("logs/remote/timely/schedule")
    //         .inspect("schedule")
    //         .into_rule("schedule"));

    // session.issue(
    //     Plan::source("logs/remote/timely/messages")
    //         .inspect("messages")
    //         .into_rule("messages"));

    session.issue(Command::Shutdown);
}