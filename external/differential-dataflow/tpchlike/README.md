## tpchlike: a TPCH-like evaluation of differential dataflow

[TPC-H](http://www.tpc.org/tpch/) is a database benchmark, used by serious people to evaluate things that make money. [Differential dataflow](https://github.com/frankmcsherry/differential-dataflow) is less serious, but it is still interesting to see how it performs on this class of tasks.

The evaluation here is meant to mimic the evaluation done in ["How to Win a Hot Dog Eating Contest"](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1), in which they also evaluate a TPC-H like workload adapted to a streaming setting. The idea is that instead of loading all base relations, we introduce tuples to the base relations in a streaming fashion. Specifically, they (and we) round-robin through the relations adding one tuple to each at a time. 

Our plan is to eventually implement all of the queries, each a few different ways to demonstrate the awesome flexibility of differential dataflow, and to get measurements from each reflecting the amount of time taken. 

---

The program runs with the command line

    cargo run --release -- <path> <logical_batch> <physical_batch> <query number>

and looks in `<path>` for the various TPC-H files (e.g. `lineitem.tbl`). If you don't have these files, you can grab the generator at the TPC-H link up above. The `logical_batch` argument merges rounds of input and changes the output of the computation; we try to use `1` for the most part, which acts as if each tuple were introduced independently. The `physical_batch` argument indicates how many logical rounds should be introduced concurrently; increasing this argument can increase the throughput at the expense of latency, but will not change the output of the computation.

Here are some throughput measurements on the scale factor 10 dataset (about 10GB of data, and sixty million tuples in the `lineitem` relation), as we vary the physical batching (varying the concurrent work) from 1K elements to 1M elements. We also list the reported from the single-threaded implementation from the hot dog eating paper. These are intended for *qualitative* comparison; so that we can see where things appear to be much improved (e.g. `q15`, `q19`, `q20`, `q22`), and where there is space to improve ourselves (e.g. `q04`, `q06`). 

|                                     |        1K |        1M | [Hot Dog](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1) |
|------------------------------------:|----------:|----------:|----------:|
| [query01](./src/queries/query01.rs) |   3.76M/s |   2.67M/s |   1.27M/s |
| [query02](./src/queries/query02.rs) |   1.80M/s |   3.46M/s | 756.61K/s |
| [query03](./src/queries/query03.rs) |   3.85M/s |   8.35M/s |   3.74M/s |
| [query04](./src/queries/query04.rs) |   3.00M/s |   4.47M/s |  10.08M/s |
| [query05](./src/queries/query05.rs) |   2.22M/s |   5.04M/s | 584.26K/s |
| [query06](./src/queries/query06.rs) |  22.77M/s |  65.23M/s | 138.33M/s |
| [query07](./src/queries/query07.rs) |   2.02M/s |   5.77M/s | 650.65K/s |
| [query08](./src/queries/query08.rs) |   1.15M/s |   2.82M/s |  91.22K/s |
| [query09](./src/queries/query09.rs) | 896.25K/s |   2.12M/s | 104.37K/s |
| [query10](./src/queries/query10.rs) |   3.05M/s |   9.00M/s |   2.89M/s |
| [query11](./src/queries/query11.rs) |   7.02K/s |   7.90K/s |     768/s |
| [query12](./src/queries/query12.rs) |   7.37M/s |  17.41M/s |   8.68M/s |
| [query13](./src/queries/query13.rs) | 528.03K/s | 892.74K/s | 779.52K/s |
| [query14](./src/queries/query14.rs) |   9.05M/s |  33.43M/s |  33.04M/s |
| [query15](./src/queries/query15.rs) |   1.52M/s |   6.39M/s |      17/s |
| [query16](./src/queries/query16.rs) |   1.82M/s |   2.91M/s | 123.94K/s |
| [query17](./src/queries/query17.rs) |   1.18M/s |   2.41M/s | 379.30K/s |
| [query18](./src/queries/query18.rs) |   3.28M/s |   4.95M/s |   1.13M/s |
| [query19](./src/queries/query19.rs) |   7.48M/s |  24.61M/s |   1.95M/s |
| [query20](./src/queries/query20.rs) |   4.12M/s |  10.39M/s |     977/s |
| [query21](./src/queries/query21.rs) | 734.42K/s |   1.39M/s | 836.80K/s |
| [query22](./src/queries/query22.rs) |  21.33K/s |  11.61K/s |     189/s |

**PLEASE NOTE**: These times are the reported running times of the code in the repository, which may or may not compute the intended quantities. It is very possible (likely, even) that I have botched some or all of the query implementations. I'm not validating the results at the moment, as I don't have much to validate against, but if you think you see bugs (or want to help validating) drop me a line! Please don't just go and use these measurements as "truth" until we find out if I am actually computing the correct answers.
