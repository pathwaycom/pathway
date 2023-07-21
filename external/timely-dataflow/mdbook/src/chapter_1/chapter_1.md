# Chapter 1: Core Concepts

Timely dataflow relies on two fundamental concepts: **timestamps** and **dataflow**, which together lead to the concept of **progress**. We will want to break down these concepts because they play a fundamental role in understanding how timely dataflow programs are structured.

## Dataflow

Dataflow programming is fundamentally about describing your program as independent components, each of which operate in response to the availability of input data, as well as describing the connections between these components.

The most important part of dataflow programming is the *independence* of the components. When you write a dataflow program, you provide the computer with flexibility in how it executes your program. Rather than insisting on a specific sequence of instructions the computer should follow, the computer can work on each of the components as it sees fit, perhaps even sharing the work with other computers.

## Timestamps

While we want to enjoy the benefits of dataflow programming, we still need to understand whether and how our computation progresses. In traditional imperative programming we could reason that because instructions happen in some order, then once we reach a certain point all work (of a certain type) must be done. Instead, we will tag the data that move through our dataflow with *timestamps*, indicating (roughly) when they would have happened in a sequential execution.

Timestamps play at least two roles in timely dataflow: they allow dataflow components to make sense of the otherwise unordered inputs they see ("ah, I received the data in *this* order, but I should behave as if it arrived in *this* order"), and they allow the user (and others) to reason about whether they have seen all of the data with a certain timestamp.

Timestamps allow us to introduce sequential structure into our program, without requiring actual sequential execution.

## Progress

In a traditional imperative program, if we want to return the maximum of a set of numbers, we just scan all the numbers and return the maximum. We don't have to worry about whether we've considered *all* of the numbers yet, because the program makes sure not to provide an answer until it has consulted each number.

This simple task is much harder in a dataflow setting, where numbers arrive as input to a component that is tracking the maximum. Before releasing a number as output, the component must know if it has seen everything, as one more value could change its answer. But strictly speaking, nothing we've said so far about dataflow or timestamps provide any information about whether more data might arrive.

If we combine dataflow program structure with timestamped data in such a way that as data move along the dataflow their timestamps only increase, we are able to reason about the *progress* of our computation. More specifically, at any component in the dataflow, we can reason about which timestamps we may yet see in the future. Timestamps that are no longer possible are considered "passed", and components can react to this information as they see fit.

Continual information about the progress of a computation is the only basis of coordination in timely dataflow, and is the lightest touch we could think of.
