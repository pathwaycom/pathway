## Step 2: Change its input.

We've written a program, one that produces skip-level reports from some `manages` relation. Let's see how we can *change* its input, and what the corresponding output changes will be.

Our organization has gone from one where each manager has at most two reports, to one where each manager has three reports. Of course, this doesn't happen overnight; each day one of the employees will switch from their old manager to their new manager. Of course, the boss gets to stay the boss, because that is what boss means.

The only change we'll make is to add the following just after we load up our initial org chart:

```rust,no_run
    for person in 1 .. size {
        input.advance_to(person);
        input.remove((person/2, person));
        input.insert((person/3, person));
    }
```

This moves us through new times, indicated by the line

```rust,no_run
        input.advance_to(person);
```

which advances the state of the `input` collection up to a timestamp `person`, which just happens to be integers that are conveniently just larger than the time `0` we used to load the data.

Once we've advanced the time, we make some changes.

```rust,no_run
        input.remove((person/2, person));
        input.insert((person/3, person));
```

This removes the prior management relation, and introduces a new one where the person reports to their newer, more over-worked manager.

We do this for each of the non-boss employees and get to see a bunch of outputs.

        Echidnatron% cargo run -- 10
             Running `target/debug/my_project`
            ((0, (0, 0)), 0, 1)
            ((0, (0, 1)), 0, 1)
            ((0, (0, 2)), 2, 1)
            ((1, (0, 2)), 0, 1)
            ((1, (0, 2)), 2, -1)
            ((1, (0, 3)), 0, 1)
            ((1, (0, 4)), 4, 1)
            ((1, (0, 5)), 5, 1)
            ((2, (0, 4)), 2, 1)
            ((2, (0, 4)), 4, -1)
            ((2, (0, 5)), 2, 1)
            ((2, (0, 5)), 5, -1)
            ((2, (0, 6)), 6, 1)
            ((2, (0, 7)), 7, 1)
            ((2, (0, 8)), 8, 1)
            ((2, (1, 4)), 0, 1)
            ((2, (1, 4)), 2, -1)
            ((2, (1, 5)), 0, 1)
            ((2, (1, 5)), 2, -1)
            ((3, (1, 6)), 0, 1)
            ((3, (1, 6)), 6, -1)
            ((3, (1, 7)), 0, 1)
            ((3, (1, 7)), 7, -1)
            ((3, (1, 9)), 9, 1)
            ((4, (1, 8)), 4, 1)
            ((4, (1, 8)), 8, -1)
            ((4, (1, 9)), 4, 1)
            ((4, (1, 9)), 9, -1)
            ((4, (2, 8)), 0, 1)
            ((4, (2, 8)), 4, -1)
            ((4, (2, 9)), 0, 1)
            ((4, (2, 9)), 4, -1)
        Echidnatron%

Gaaaaaaah! What in the !#$!?

It turns out our input changes result in output changes. Let's try and break this down and make some sense. If we group the columns by time, the second element of the tuples, we see a bit more structure.

1. The entries with time `0` are exactly the same as for our prior computation, where we just loaded the data.

2. There aren't any entries at time `1` (go check). That is because the input didn't change in our first step, because 1/2 == 1/3 == 0. Since the input didn't change, the output doesn't change.

3. The other times are more complicated.

Let's look at the entries for time `4`.

        ((1, (0, 4)), 4, 1)
        ((2, (0, 4)), 4, -1)
        ((4, (1, 8)), 4, 1)
        ((4, (1, 9)), 4, 1)
        ((4, (2, 8)), 4, -1)
        ((4, (2, 9)), 4, -1)

There is a bit going on here. Four's manager changed from two to one, and while their skip-level manager remained zero the explanation changed. The first two lines record this change. The next four lines record the change in the skip-level manager of four's reports, eight and nine.

At the end, time `9`, things are a bit simpler because we have reached the employees with no reports, and so the only changes are their skip-level manager, without any implications for other people.

        ((3, (1, 9)), 9, 1)
        ((4, (1, 9)), 9, -1)

Oof. Well, we probably *could* have figured these things out by hand, right?

Let's check out some ways this gets more interesting.