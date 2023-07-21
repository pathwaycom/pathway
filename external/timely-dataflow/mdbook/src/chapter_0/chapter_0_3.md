# When not to use Timely Dataflow

There are several reasons not to use timely dataflow, though many of them are *friction* about how your problem is probably expressed, rather than fundamental technical limitations. There are fundamental technical limitations too, of course.

I've collected a few examples here, but the list may grow with input and feedback.

---

Timely dataflow is a *dataflow* system, and this means that at its core it likes to move data around. This makes life complicated when you would prefer not to move data, and instead move things like pointers and references to data that otherwise stays put.

For example, sorting a slice of data is a fundamental task and one that parallelizes. But, the task of sorting is traditionally viewed as transforming the data in a supplied slice, rather than sending the data to multiple workers and then announcing that it got sorted. The data really does need to end up in one place, one single pre-existing memory allocation, and timely dataflow is not great at problems that cannot be recast as the movement of data.

One could re-imagine the sorting process as moving data around, and indeed this is what happens when large clusters need to be brought to bear on such a task, but that doesn't help you at all if what you needed was to sort your single allocation. A library like [Rayon](https://github.com/nikomatsakis/rayon) would almost surely be better suited to the task.

---

Dataflow systems are also fundamentally about breaking apart the execution of your program into independently operating parts. However, many programs are correct only because some things happen *before* or *after* other things. A classic example is [depth-first search](https://en.wikipedia.org/wiki/Depth-first_search) in a graph: although there is lots of work to do on small bits of data, it is crucial that the exploration of nodes reachable along a graph edge complete before the exploration of nodes reachable along the next graph edge.

Although there is plenty of active research on transforming algorithms from sequential to parallel, if you aren't clear on how to express your program as a dataflow program then timely dataflow may not be a great fit. At the very least, the first step would be "fundamentally re-imagine your program", which can be a fine thing to do, but is perhaps not something you would have to do with your traditional program.

---

Timely dataflow is in a bit of a weird space between language library and runtime system. This means that it doesn't quite have the stability guarantees a library might have (when you call `data.sort()` you don't think about "what if it fails?"), nor does it have the surrounding infrastructure of a [DryadLINQ](https://www.microsoft.com/en-us/research/project/dryadlinq/) or [Spark](https://spark.apache.org) style of experience. Part of this burden is simply passed to you, and this may be intolerable depending on your goals for your program.
