# When to use Timely Dataflow

Timely dataflow may be a different programming model than you are used to, but if you can adapt your program to it there are several benefits.

* **Data Parallelism**: The operators in timely dataflow are largely "data-parallel", meaning they can operate on independent parts of the data concurrently. This allows the underlying system to distribute timely dataflow computations across multiple parallel workers. These can be threads on your computer, or even threads across computers in a cluster you have access to. This distribution typically improves the throughput of the system, and lets you scale to larger problems with access to more resources (computation, communication, and memory).

* **Streaming Data**: The core data type in timely dataflow is a *stream* of data, an unbounded collection of data not all of which is available right now, but which instead arrives as the computation proceeds. Streams are a helpful generalization of static data sets, which are assumed available at the start of the computation. By expressing your program as a computation on streams, you've explained both how it should respond to static input data sets (feed all the data in at once) but also how it should react to new data that might arrive later on.

* **Expressivity**: Timely dataflow's main addition over traditional stream processors is its ability to express higher-level control constructs, like iteration. This moves stream computations from the limitations of straight line code to the world of *algorithms*. Many of the advantages of timely dataflow computations come from our ability to express a more intelligent algorithm than the alternative systems, which can only express more primitive computations.

There are many domains where streaming and scalability are important, and I'm not sure I can name them all. If you would like to build a scalable monitoring application for a service you run, timely dataflow can express this. If you would like to work with big data computations processing more data than your computer can load into memory, timely dataflow streams can represent this efficiently. If you would like to build an incremental iterative computation over massive data (e.g. matrices, large graphs, text corpora), timely dataflow has done these things.

At the same time, dataflow computation is also another way of thinking about your program. Much like Rust causes you to think a bit harder about program structure, timely dataflow helps you tease out some structure to your program that results in a more effective computation. Even when writing something like `grep`, a program that scans lines of text looking for patterns, by stating your program as a dataflow computation its implementation immediately scales out to multiple threads, and even across multiple computers.

## Generality

Is timely dataflow always applicable? The intent of this research project is to remove layers of abstraction fat that prevent you from expressing anything your computer can do efficiently in parallel.

Under the covers, your computer (the one on which you are reading this text) is a dataflow processor. When your computer *reads memory* it doesn't actually wander off to find the memory, it introduces a read request into your memory controller, an independent component that will eventually return with the associated cache line. Your computer then gets back to work on whatever it was doing, hoping the responses from the controller return in a timely fashion.

Academically, I treat "my computer can do this, but timely dataflow cannot" as a bug. There are degrees, of course, and timely dataflow isn't on par with the processor's custom hardware designed to handle low level requests efficiently, but *algorithmically*, the goal is that anything you can do efficiently with a computer you should be able to express in timely dataflow.
