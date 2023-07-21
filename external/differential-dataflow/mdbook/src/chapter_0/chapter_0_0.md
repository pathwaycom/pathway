## Getting started

The first thing you will need to do, if you want to follow along with the examples, is to acquire a copy of [Rust](https://www.rust-lang.org/). This is the programming language that differential dataflow uses, and it is in charge of building our projects.

With Rust in hand, crack open a shell and make a new project using Rust build manager `cargo`.

        Echidnatron% cargo new my_project

This should create a new folder called `my_project`, and you can wander in there and type

        Echidnatron% cargo run

This will do something reassuring but pointless, like print `Hello, world!`, because we haven't gotten differential dataflow involved yet. I mean, it's Rust and you could learn that, but you probably want to read a different web page in that case.

Instead, edit your `Cargo.toml` file, which tells Rust about your dependencies, to look like this:

        Echidnatron% cat Cargo.toml
        [package]
        name = "my_project"
        version = "0.1.0"
        authors = ["Your Name <your_name@you.ch>"]

        [dependencies]
        timely = "0.11.1"
        differential-dataflow = "0.11.0"
        Echidnatron%

You should only need to add those last two lines there, which bring in dependencies on both [timely dataflow](https://github.com/TimelyDataflow/timely-dataflow) and [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow). We will be using both of those.

If you would like to point at the most current code release, hosted on github, you can replace the dependencies with:

        [dependencies]
        timely = { git = "https://github.com/TimelyDataflow/timely-dataflow" }
        differential-dataflow = { git = "https://github.com/TimelyDataflow/differential-dataflow" }


You should now be ready to go. Code examples should mostly work, and you should complain (or [file an issue](https://github.com/TimelyDataflow/differential-dataflow/issues)) if they do not!
