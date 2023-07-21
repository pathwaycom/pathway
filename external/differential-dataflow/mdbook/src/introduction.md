# Differential Dataflow

In this book we will work through the motivation and technical details behind [differential dataflow](https://github.com/frankmcsherry/differential-dataflow), a computational framework build on top of [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) intended for efficiently performing computations on large amounts of data and *maintaining* the computations as the data change.

Differential dataflow programs look like many standard "big data" computations, borrowing idioms from frameworks like MapReduce and SQL. However, once you write and run your program, you can *change* the data inputs to the computation, and differential dataflow will promptly show you the corresponding changes in its output. Promptly meaning in as little as milliseconds.

This relatively simple set-up, write programs and then change inputs, leads to a surprising breadth of exciting and new classes of scalable computation. We will explore it in this document!

---

Differential dataflow arose from [work at Microsoft Research](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/11/naiad_sosp2013.pdf), where we aimed to build a high-level framework that could both compute and incrementally maintain non-trivial algorithms.