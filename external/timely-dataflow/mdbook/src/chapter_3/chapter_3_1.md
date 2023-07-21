# Providing Input

The first thing we often see is `input.send` with some data. This moves the supplied data from the current scope into a queue shared with the input dataflow operator. As this queue starts to fill, perhaps due to you calling `send` a lot, it moves the data along to its intended recipients. This probably means input queues of other operators, but it may mean serialization and network transmission.

You can call `send` as much as you like, and the `InputHandle` implementation will keep moving the data along. The worst that is going to happen is depositing data in shared queues and perhaps some serialization; the call to `send` will not block, and it should not capture your thread of execution to do any particularly expensive work.

However, since we are writing the worker code, you may want to take a break from `send` every now and again and let some of the operators run (in just a moment). Part of efficient streaming computation is keeping the data moving, and building up only relatively small buffers before giving the operators a chance to run.

## Controlling capabilities

The second thing we often see is `input.advance_to` with a time. This is an exciting moment where the input announces that it will no longer send data timestamped with anything not greater or equal to its argument. This is big news for the rest of the system, as any operator waiting on the timestamp you previously held can now get to work (or, once all the messages you sent have drained, it can get to work).

It is a logic error to call `advance_to` with a time that is not greater or equal to the current time, which you can read out with `input.time`. Timely will check this for you and panic if you screw it up. It is a bit like accessing an array out of bounds: you can check ahead of time if you are about to screw up, but you went and did it anyhow.

Finally, you might be interested to call `input.close`. This method consumes the input and thereby prevents you from sending any more data. This information is *very* exciting to the system, which can now tell dataflow operators that they won't be hearing much of anything from you any more.

**TIP**: It is very important to keep moving your inputs along if you want your dataflow graph to make progress. One of the most common classes of errors is forgetting to advance an `InputHandle`, and then waiting and waiting and waiting for the cumulative count of records (or whatever) to come out the other end. Timely really wants you to participate and be clear about what you will and will not do in the future.

At the same time, timely's progress tracking does work proportional to the number of timestamps you introduce. If you use a new timestamp for every record, timely will flush its buffers a lot, get very angry with you, and probably fall over. To the extent that you can batch inputs, sending many with the same timestamp, the better.
