# When to use Differential Dataflow

Differential dataflow is an "opinionated" programming framework, meaning that it aims to accommodate certain tasks and is unapologetic about its relatively uselessness for other tasks. To become delighted with differential dataflow it is important to set expectations appropriately, and to do that we need to understand what it tries to do well.

Differential dataflow is aimed at combinatorial algorithms over collections, a typical example being large scale graph computation, perhaps computing and maintaining the set of connected components in a graph. As we will see, this covers a fair bit of ground, from standard big data computations like SQL and MapReduce, up through deductive reasoning systems and some forms of structured machine learning.

* **Functional programming**: Differential dataflow operators are *functional*, in that they transform their inputs into outputs without modifying the inputs themselves. Functional operators (and programs) allow us to more easily understand the changes a program undergoes when its input changes. While functional programming is restrictive, the benefits we get in exchange (efficient distributed, iterative, and incremental execution) are powerful and potentially worth the cognitive shift.

* **Data Parallelism**: Differential dataflow operators are largely "data-parallel", meaning that they can operate on disjoint parts of the input independently. This allows many big data frameworks to distribute work across multiple workers, which we do as well, but perhaps more importantly it allows us to constrain the flow of updates and only perform *re*-computation for values that have changed.

* **Iteration**: The most significant technical departure from prior work is the ability to compute and maintain iterative computations. This is unlike most database and big data processors. The ability to perform efficient iteration allows us to support computations with non-trivial control flow.

* **Incremental updates**: Differential dataflow maintains computations as their inputs change. This maintenance work is often substantially lower than the cost to fully re-evaluate a computation from scratch. Differential dataflow is specifically engineered to provide both high throughput and low latency, at the same time.

These features come together to provide a platform with functionality not yet found in other solutions. If your problem requires these features, or would benefit from them at all, differential dataflow is worth investigating.

The scope of problems well-accommodated by differential dataflow is only growing with time, as we learn more about problems where these needs align, and implement more algorithms and operators in differential dataflow. Ideally this document will give you some ideas about how to find uses in your areas of interest.