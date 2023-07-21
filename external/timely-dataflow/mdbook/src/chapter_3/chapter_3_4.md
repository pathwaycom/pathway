# Extending Dataflows

This might be surprising to see in the "Running Timely Dataflows" section, but it is worth pointing out. Just because we built one dataflow doesn't mean that we have to stop there. We can run one dataflow for a while, and then create a second dataflow and run it. While we are running those (just one `worker.step()` calls into both) we could create a third and start running that too.

The `worker` can track an arbitrary number of dataflows, and will clean up after each of them once it is complete (when no capabilities exist in the dataflow). You are welcome to spin up as many as you like, if there are reasons you might need several in the course of one program.

You can also do something fun that we're working on (in progress), which is to map in shared libraries and load the dataflows they define. This gives rise to something like a "timely dataflow cluster" that can accept jobs (in the form of shared libraries) which are installed and run sharing the resources with other dataflows. Of course, if they crash they take down everything, so bear this in mind before getting too excited.
