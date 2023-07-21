extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan};
use interactive::concrete::{Session, Value};

fn main() {

    let socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");
    let mut session = Session::new(socket);

    // Create initially empty set of edges.
    session.issue(Command::CreateInput("Edges".to_string(), Vec::new()));

    for node in 0 .. 1000 {
        let edge = vec![Value::Usize(node), Value::Usize(node+1)];
        session.issue(Command::UpdateInput("Edges".to_string(), vec![(edge, Duration::from_secs(0), 1)]));
    }

    // Create initially empty set of edges.
    session.issue(Command::CreateInput("Nodes".to_string(), Vec::new()));

    session.issue(
        Plan::source("Nodes")
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .inspect("one-hop")
            .into_rule("One-hop"));

    session.issue(Command::AdvanceTime(Duration::from_secs(1)));
    session.issue(Command::UpdateInput("Nodes".to_string(), vec![(vec![Value::Usize(0)], Duration::from_secs(1), 1)]));
    session.issue(Command::AdvanceTime(Duration::from_secs(2)));

    session.issue(
        Plan::source("Nodes")
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges"), vec![(0, 0)])
            .project(vec![1])
            .inspect("ten-hop")
            .into_rule("Ten-hop"));

    session.issue(Command::AdvanceTime(Duration::from_secs(3)));
    session.issue(Command::Shutdown);
}