//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging
//! immutable batches of updates. It is generic with respect to the batch type, and can be
//! instantiated for any implementor of `trace::Batch`.
//!
//! ## Design
//!
//! This spine is represented as a list of layers, where each element in the list is either
//!
//!   1. MergeState::Vacant  empty
//!   2. MergeState::Single  a single batch
//!   3. MergeState::Double  a pair of batches
//!
//! Each "batch" has the option to be `None`, indicating a non-batch that nonetheless acts
//! as a number of updates proportionate to the level at which it exists (for bookkeeping).
//!
//! Each of the batches at layer i contains at most 2^i elements. The sequence of batches
//! should have the upper bound of one match the lower bound of the next. Batches may be
//! logically empty, with matching upper and lower bounds, as a bookkeeping mechanism.
//!
//! Each batch at layer i is treated as if it contains exactly 2^i elements, even though it
//! may actually contain fewer elements. This allows us to decouple the physical representation
//! from logical amounts of effort invested in each batch. It allows us to begin compaction and
//! to reduce the number of updates, without compromising our ability to continue to move
//! updates along the spine. We are explicitly making the trade-off that while some batches
//! might compact at lower levels, we want to treat them as if they contained their full set of
//! updates for accounting reasons (to apply work to higher levels).
//!
//! We maintain the invariant that for any in-progress merge at level k there should be fewer
//! than 2^k records at levels lower than k. That is, even if we were to apply an unbounded
//! amount of effort to those records, we would not have enough records to prompt a merge into
//! the in-progress merge. Ideally, we maintain the extended invariant that for any in-progress
//! merge at level k, the remaining effort required (number of records minus applied effort) is
//! less than the number of records that would need to be added to reach 2^k records in layers
//! below.
//!
//! ## Mathematics
//!
//! When a merge is initiated, there should be a non-negative *deficit* of updates before the layers
//! below could plausibly produce a new batch for the currently merging layer. We must determine a
//! factor of proportionality, so that newly arrived updates provide at least that amount of "fuel"
//! towards the merging layer, so that the merge completes before lower levels invade.
//!
//! ### Deficit:
//!
//! A new merge is initiated only in response to the completion of a prior merge, or the introduction
//! of new records from outside. The latter case is special, and will maintain our invariant trivially,
//! so we will focus on the former case.
//!
//! When a merge at level k completes, assuming we have maintained our invariant then there should be
//! fewer than 2^k records at lower levels. The newly created merge at level k+1 will require up to
//! 2^k+2 units of work, and should not expect a new batch until strictly more than 2^k records are
//! added. This means that a factor of proportionality of four should be sufficient to ensure that
//! the merge completes before a new merge is initiated.
//!
//! When new records get introduced, we will need to roll up any batches at lower levels, which we
//! treat as the introduction of records. Each of these virtual records introduced should either be
//! accounted for the fuel it should contribute, as it results in the promotion of batches closer to
//! in-progress merges.
//!
//! ### Fuel sharing
//!
//! We like the idea of applying fuel preferentially to merges at *lower* levels, under the idea that
//! they are easier to complete, and we benefit from fewer total merges in progress. This does delay
//! the completion of merges at higher levels, and may not obviously be a total win. If we choose to
//! do this, we should make sure that we correctly account for completed merges at low layers: they
//! should still extract fuel from new updates even though they have completed, at least until they
//! have paid back any "debt" to higher layers by continuing to provide fuel as updates arrive.


use std::fmt::Debug;

use ::logging::Logger;
use ::difference::Semigroup;
use lattice::Lattice;
use trace::{Batch, BatchReader, Trace, TraceReader};
use trace::cursor::{Cursor, CursorList};
use trace::Merger;

use ::timely::dataflow::operators::generic::OperatorInfo;
use ::timely::progress::{Antichain, frontier::AntichainRef};
use ::timely::order::PartialOrder;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections.
pub struct Spine<B: Batch> where B::Time: Lattice+Ord, B::R: Semigroup {
    operator: OperatorInfo,
    logger: Option<Logger>,
    pub(crate) logical_frontier: Antichain<B::Time>,   // Times after which the trace must accumulate correctly.
    pub(crate) physical_frontier: Antichain<B::Time>,  // Times after which the trace must be able to subset its inputs.
    pub(crate) merging: Vec<MergeState<B>>,            // Several possibly shared collections of updates.
    pub(crate) pending: Vec<B>,                        // Batches at times in advance of `frontier`.
    upper: Antichain<B::Time>,
    effort: usize,
    activator: Option<timely::scheduling::activate::Activator>,
}

impl<B> TraceReader for Spine<B>
where
    B: Batch+Clone+'static,
    B::Key: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    B::Val: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    B::Time: Lattice+timely::progress::Timestamp+Ord+Clone+Debug,
    B::R: Semigroup,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Batch = B;
    type Cursor = CursorList<<B as BatchReader>::Cursor>;

    fn cursor_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<(Self::Cursor, <Self::Cursor as Cursor>::Storage)> {

        // If `upper` is the minimum frontier, we can return an empty cursor.
        // This can happen with operators that are written to expect the ability to acquire cursors
        // for their prior frontiers, and which start at `[T::minimum()]`, such as `Reduce`, sadly.
        if upper.less_equal(&<Self::Time as timely::progress::Timestamp>::minimum()) {
            let cursors = Vec::new();
            let storage = Vec::new();
            return Some((CursorList::new(cursors, &storage), storage));
        }

        // The supplied `upper` should have the property that for each of our
        // batch `lower` and `upper` frontiers, the supplied upper is comparable
        // to the frontier; it should not be incomparable, because the frontiers
        // that we created form a total order. If it is, there is a bug.
        //
        // We should acquire a cursor including all batches whose upper is less
        // or equal to the supplied upper, excluding all batches whose lower is
        // greater or equal to the supplied upper, and if a batch straddles the
        // supplied upper it had better be empty.

        // We shouldn't grab a cursor into a closed trace, right?
        assert!(self.logical_frontier.borrow().len() > 0);

        // Check that `upper` is greater or equal to `self.physical_frontier`.
        // Otherwise, the cut could be in `self.merging` and it is user error anyhow.
        // assert!(upper.iter().all(|t1| self.physical_frontier.iter().any(|t2| t2.less_equal(t1))));
        assert!(PartialOrder::less_equal(&self.physical_frontier.borrow(), &upper));

        let mut cursors = Vec::new();
        let mut storage = Vec::new();

        for merge_state in self.merging.iter().rev() {
            match merge_state {
                MergeState::Double(variant) => {
                    match variant {
                        MergeVariant::InProgress(batch1, batch2, _) => {
                            if !batch1.is_empty() {
                                cursors.push(batch1.cursor());
                                storage.push(batch1.clone());
                            }
                            if !batch2.is_empty() {
                                cursors.push(batch2.cursor());
                                storage.push(batch2.clone());
                            }
                        },
                        MergeVariant::Complete(Some((batch, _))) => {
                            if !batch.is_empty() {
                                cursors.push(batch.cursor());
                                storage.push(batch.clone());
                            }
                        }
                        MergeVariant::Complete(None) => { },
                    }
                },
                MergeState::Single(Some(batch)) => {
                    if !batch.is_empty() {
                        cursors.push(batch.cursor());
                        storage.push(batch.clone());
                    }
                },
                MergeState::Single(None) => { },
                MergeState::Vacant => { },
            }
        }

        for batch in self.pending.iter() {

            if !batch.is_empty() {

                // For a non-empty `batch`, it is a catastrophic error if `upper`
                // requires some-but-not-all of the updates in the batch. We can
                // determine this from `upper` and the lower and upper bounds of
                // the batch itself.
                //
                // TODO: It is not clear if this is the 100% correct logic, due
                // to the possible non-total-orderedness of the frontiers.

                let include_lower = PartialOrder::less_equal(&batch.lower().borrow(), &upper);
                let include_upper = PartialOrder::less_equal(&batch.upper().borrow(), &upper);

                if include_lower != include_upper && upper != batch.lower().borrow() {
                    panic!("`cursor_through`: `upper` straddles batch");
                }

                // include pending batches
                if include_upper {
                    cursors.push(batch.cursor());
                    storage.push(batch.clone());
                }
            }
        }

        Some((CursorList::new(cursors, &storage), storage))
    }
    fn set_logical_compaction(&mut self, frontier: AntichainRef<B::Time>) {
        self.logical_frontier.clear();
        self.logical_frontier.extend(frontier.iter().cloned());
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<B::Time> { self.logical_frontier.borrow() }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<B::Time>) {
        // We should never request to rewind the frontier.
        debug_assert!(PartialOrder::less_equal(&self.physical_frontier.borrow(), &frontier), "FAIL\tthrough frontier !<= new frontier {:?} {:?}\n", self.physical_frontier, frontier);
        self.physical_frontier.clear();
        self.physical_frontier.extend(frontier.iter().cloned());
        self.consider_merges();
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<B::Time> { self.physical_frontier.borrow() }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        for batch in self.merging.iter().rev() {
            match batch {
                MergeState::Double(MergeVariant::InProgress(batch1, batch2, _)) => { f(batch1); f(batch2); },
                MergeState::Double(MergeVariant::Complete(Some((batch, _)))) => { f(batch) },
                MergeState::Single(Some(batch)) => { f(batch) },
                _ => { },
            }
        }
        for batch in self.pending.iter() {
            f(batch);
        }
    }
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and batch types.
impl<B> Trace for Spine<B>
where
    B: Batch+Clone+'static,
    B::Key: Ord+Clone,
    B::Val: Ord+Clone,
    B::Time: Lattice+timely::progress::Timestamp+Ord+Clone+Debug,
    B::R: Semigroup,
{
    fn new(
        info: ::timely::dataflow::operators::generic::OperatorInfo,
        logging: Option<::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self {
        Self::with_effort(1, info, logging, activator)
    }

    /// Apply some amount of effort to trace maintenance.
    ///
    /// The units of effort are updates, and the method should be
    /// thought of as analogous to inserting as many empty updates,
    /// where the trace is permitted to perform proportionate work.
    fn exert(&mut self, effort: &mut isize) {
        // If there is work to be done, ...
        self.tidy_layers();
        if !self.reduced() {

            // If any merges exist, we can directly call `apply_fuel`.
            if self.merging.iter().any(|b| b.is_double()) {
                self.apply_fuel(effort);
            }
            // Otherwise, we'll need to introduce fake updates to move merges along.
            else {
                // Introduce an empty batch with roughly *effort number of virtual updates.
                let level = (*effort as usize).next_power_of_two().trailing_zeros() as usize;
                self.introduce_batch(None, level);
            }
            // We were not in reduced form, so let's check again in the future.
            if let Some(activator) = &self.activator {
                activator.activate();
            }
        }
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet able to begin
    // merging the batch. This means it is a good time to perform amortized work proportional
    // to the size of batch.
    fn insert(&mut self, batch: Self::Batch) {

        // Log the introduction of a batch.
        self.logger.as_ref().map(|l| l.log(::logging::BatchEvent {
            operator: self.operator.global_id,
            length: batch.len()
        }));

        assert!(batch.lower() != batch.upper());
        assert_eq!(batch.lower(), &self.upper);

        self.upper.clone_from(batch.upper());

        // TODO: Consolidate or discard empty batches.
        self.pending.push(batch);
        self.consider_merges();
    }

    /// Completes the trace with a final empty batch.
    fn close(&mut self) {
        if !self.upper.borrow().is_empty() {
            use trace::Builder;
            let builder = B::Builder::new();
            let batch = builder.done(self.upper.clone(), Antichain::new(), Antichain::from_elem(<Self::Time as timely::progress::Timestamp>::minimum()));
            self.insert(batch);
        }
    }
}

// Drop implementation allows us to log batch drops, to zero out maintained totals.
impl<B> Drop for Spine<B>
where
    B: Batch,
    B::Time: Lattice+Ord,
    B::R: Semigroup,
{
    fn drop(&mut self) {
        self.drop_batches();
    }
}


impl<B> Spine<B>
where
    B: Batch,
    B::Time: Lattice+Ord,
    B::R: Semigroup,
{
    /// Drops and logs batches. Used in `set_logical_compaction` and drop.
    fn drop_batches(&mut self) {
        if let Some(logger) = &self.logger {
            for batch in self.merging.drain(..) {
                match batch {
                    MergeState::Single(Some(batch)) => {
                        logger.log(::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: batch.len(),
                        });
                    },
                    MergeState::Double(MergeVariant::InProgress(batch1, batch2, _)) => {
                        logger.log(::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: batch1.len(),
                        });
                        logger.log(::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: batch2.len(),
                        });
                    },
                    MergeState::Double(MergeVariant::Complete(Some((batch, _)))) => {
                        logger.log(::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: batch.len(),
                        });
                    }
                    _ => { },
                }
            }
            for batch in self.pending.drain(..) {
                logger.log(::logging::DropEvent {
                    operator: self.operator.global_id,
                    length: batch.len(),
                });
            }
        }
    }
}

impl<B> Spine<B>
where
    B: Batch,
    B::Key: Ord+Clone,
    B::Val: Ord+Clone,
    B::Time: Lattice+timely::progress::Timestamp+Ord+Clone+Debug,
    B::R: Semigroup,
{
    /// True iff there is at most one non-empty batch in `self.merging`.
    ///
    /// When true, there is no maintenance work to perform in the trace, other than compaction.
    /// We do not yet have logic in place to determine if compaction would improve a trace, so
    /// for now we are ignoring that.
    fn reduced(&self) -> bool {
        let mut non_empty = 0;
        for index in 0 .. self.merging.len() {
            if self.merging[index].is_double() { return false; }
            if self.merging[index].len() > 0 { non_empty += 1; }
            if non_empty > 1 { return false; }
        }
        true
    }

    /// Describes the merge progress of layers in the trace.
    ///
    /// Intended for diagnostics rather than public consumption.
    #[allow(dead_code)]
    fn describe(&self) -> Vec<(usize, usize)> {
        self.merging
            .iter()
            .map(|b| match b {
                MergeState::Vacant => (0, 0),
                x @ MergeState::Single(_) => (1, x.len()),
                x @ MergeState::Double(_) => (2, x.len()),
            })
            .collect()
    }

    /// Allocates a fueled `Spine` with a specified effort multiplier.
    ///
    /// This trace will merge batches progressively, with each inserted batch applying a multiple
    /// of the batch's length in effort to each merge. The `effort` parameter is that multiplier.
    /// This value should be at least one for the merging to happen; a value of zero is not helpful.
    pub fn with_effort(
        mut effort: usize,
        operator: OperatorInfo,
        logger: Option<::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self {

        // Zero effort is .. not smart.
        if effort == 0 { effort = 1; }

        Spine {
            operator,
            logger,
            logical_frontier: Antichain::from_elem(<B::Time as timely::progress::Timestamp>::minimum()),
            physical_frontier: Antichain::from_elem(<B::Time as timely::progress::Timestamp>::minimum()),
            merging: Vec::new(),
            pending: Vec::new(),
            upper: Antichain::from_elem(<B::Time as timely::progress::Timestamp>::minimum()),
            effort,
            activator,
        }
    }

    /// Migrate data from `self.pending` into `self.merging`.
    ///
    /// This method reflects on the bookmarks held by others that may prevent merging, and in the
    /// case that new batches can be introduced to the pile of mergeable batches, it gets on that.
    #[inline(never)]
    fn consider_merges(&mut self) {

        // TODO: Consider merging pending batches before introducing them.
        // TODO: We could use a `VecDeque` here to draw from the front and append to the back.
        while self.pending.len() > 0 && PartialOrder::less_equal(self.pending[0].upper(), &self.physical_frontier)
            //   self.physical_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1)))
        {
            // Batch can be taken in optimized insertion.
            // Otherwise it is inserted normally at the end of the method.
            let mut batch = Some(self.pending.remove(0));

            // If `batch` and the most recently inserted batch are both empty, we can just fuse them.
            // We can also replace a structurally empty batch with this empty batch, preserving the
            // apparent record count but now with non-trivial lower and upper bounds.
            if batch.as_ref().unwrap().len() == 0 {
                if let Some(position) = self.merging.iter().position(|m| !m.is_vacant()) {
                    if self.merging[position].is_single() && self.merging[position].len() == 0 {
                        self.insert_at(batch.take(), position);
                        let merged = self.complete_at(position);
                        self.merging[position] = MergeState::Single(merged);
                    }
                }
            }

            // Normal insertion for the batch.
            if let Some(batch) = batch {
                let index = batch.len().next_power_of_two();
                self.introduce_batch(Some(batch), index.trailing_zeros() as usize);
            }
        }

        // Having performed all of our work, if more than one batch remains reschedule ourself.
        if !self.reduced() {
            if let Some(activator) = &self.activator {
                activator.activate();
            }
        }
    }

    /// Introduces a batch at an indicated level.
    ///
    /// The level indication is often related to the size of the batch, but
    /// it can also be used to artificially fuel the computation by supplying
    /// empty batches at non-trivial indices, to move merges along.
    pub fn introduce_batch(&mut self, batch: Option<B>, batch_index: usize) {

        // Step 0.  Determine an amount of fuel to use for the computation.
        //
        //          Fuel is used to drive maintenance of the data structure,
        //          and in particular are used to make progress through merges
        //          that are in progress. The amount of fuel to use should be
        //          proportional to the number of records introduced, so that
        //          we are guaranteed to complete all merges before they are
        //          required as arguments to merges again.
        //
        //          The fuel use policy is negotiable, in that we might aim
        //          to use relatively less when we can, so that we return
        //          control promptly, or we might account more work to larger
        //          batches. Not clear to me which are best, of if there
        //          should be a configuration knob controlling this.

        // The amount of fuel to use is proportional to 2^batch_index, scaled
        // by a factor of self.effort which determines how eager we are in
        // performing maintenance work. We need to ensure that each merge in
        // progress receives fuel for each introduced batch, and so multiply
        // by that as well.
        if batch_index > 32 { println!("Large batch index: {}", batch_index); }

        // We believe that eight units of fuel is sufficient for each introduced
        // record, accounted as four for each record, and a potential four more
        // for each virtual record associated with promoting existing smaller
        // batches. We could try and make this be less, or be scaled to merges
        // based on their deficit at time of instantiation. For now, we remain
        // conservative.
        let mut fuel = 8 << batch_index;
        // Scale up by the effort parameter, which is calibrated to one as the
        // minimum amount of effort.
        fuel *= self.effort;
        // Convert to an `isize` so we can observe any fuel shortfall.
        let mut fuel = fuel as isize;

        // Step 1.  Apply fuel to each in-progress merge.
        //
        //          Before we can introduce new updates, we must apply any
        //          fuel to in-progress merges, as this fuel is what ensures
        //          that the merges will be complete by the time we insert
        //          the updates.
        self.apply_fuel(&mut fuel);

        // Step 2.  We must ensure the invariant that adjacent layers do not
        //          contain two batches will be satisfied when we insert the
        //          batch. We forcibly completing all merges at layers lower
        //          than and including `batch_index`, so that the new batch
        //          is inserted into an empty layer.
        //
        //          We could relax this to "strictly less than `batch_index`"
        //          if the layer above has only a single batch in it, which
        //          seems not implausible if it has been the focus of effort.
        //
        //          This should be interpreted as the introduction of some
        //          volume of fake updates, and we will need to fuel merges
        //          by a proportional amount to ensure that they are not
        //          surprised later on. The number of fake updates should
        //          correspond to the deficit for the layer, which perhaps
        //          we should track explicitly.
        self.roll_up(batch_index);

        // Step 3. This insertion should be into an empty layer. It is a
        //         logical error otherwise, as we may be violating our
        //         invariant, from which all wonderment derives.
        self.insert_at(batch, batch_index);

        // Step 4. Tidy the largest layers.
        //
        //         It is important that we not tidy only smaller layers,
        //         as their ascension is what ensures the merging and
        //         eventual compaction of the largest layers.
        self.tidy_layers();
    }

    /// Ensures that an insertion at layer `index` will succeed.
    ///
    /// This method is subject to the constraint that all existing batches
    /// should occur at higher levels, which requires it to "roll up" batches
    /// present at lower levels before the method is called. In doing this,
    /// we should not introduce more virtual records than 2^index, as that
    /// is the amount of excess fuel we have budgeted for completing merges.
    fn roll_up(&mut self, index: usize) {

        // Ensure entries sufficient for `index`.
        while self.merging.len() <= index {
            self.merging.push(MergeState::Vacant);
        }

        // We only need to roll up if there are non-vacant layers.
        if self.merging[.. index].iter().any(|m| !m.is_vacant()) {

            // Collect and merge all batches at layers up to but not including `index`.
            let mut merged = None;
            for i in 0 .. index {
                self.insert_at(merged, i);
                merged = self.complete_at(i);
            }

            // The merged results should be introduced at level `index`, which should
            // be ready to absorb them (possibly creating a new merge at the time).
            self.insert_at(merged, index);

            // If the insertion results in a merge, we should complete it to ensure
            // the upcoming insertion at `index` does not panic.
            if self.merging[index].is_double() {
                let merged = self.complete_at(index);
                self.insert_at(merged, index + 1);
            }
        }
    }

    /// Applies an amount of fuel to merges in progress.
    ///
    /// The supplied `fuel` is for each in progress merge, and if we want to spend
    /// the fuel non-uniformly (e.g. prioritizing merges at low layers) we could do
    /// so in order to maintain fewer batches on average (at the risk of completing
    /// merges of large batches later, but tbh probably not much later).
    pub fn apply_fuel(&mut self, fuel: &mut isize) {
        // For the moment our strategy is to apply fuel independently to each merge
        // in progress, rather than prioritizing small merges. This sounds like a
        // great idea, but we need better accounting in place to ensure that merges
        // that borrow against later layers but then complete still "acquire" fuel
        // to pay back their debts.
        for index in 0 .. self.merging.len() {
            // Give each level independent fuel, for now.
            let mut fuel = *fuel;
            // Pass along various logging stuffs, in case we need to report success.
            self.merging[index].work(&mut fuel);
            // `fuel` could have a deficit at this point, meaning we over-spent when
            // we took a merge step. We could ignore this, or maintain the deficit
            // and account future fuel against it before spending again. It isn't
            // clear why that would be especially helpful to do; we might want to
            // avoid overspends at multiple layers in the same invocation (to limit
            // latencies), but there is probably a rich policy space here.

            // If a merge completes, we can immediately merge it in to the next
            // level, which is "guaranteed" to be complete at this point, by our
            // fueling discipline.
            if self.merging[index].is_complete() {
                let complete = self.complete_at(index);
                self.insert_at(complete, index+1);
            }
        }
    }

    /// Inserts a batch at a specific location.
    ///
    /// This is a non-public internal method that can panic if we try and insert into a
    /// layer which already contains two batches (and is still in the process of merging).
    fn insert_at(&mut self, batch: Option<B>, index: usize) {
        // Ensure the spine is large enough.
        while self.merging.len() <= index {
            self.merging.push(MergeState::Vacant);
        }

        // Insert the batch at the location.
        match self.merging[index].take() {
            MergeState::Vacant => {
                self.merging[index] = MergeState::Single(batch);
            }
            MergeState::Single(old) => {
                // Log the initiation of a merge.
                self.logger.as_ref().map(|l| l.log(
                    ::logging::MergeEvent {
                        operator: self.operator.global_id,
                        scale: index,
                        length1: old.as_ref().map(|b| b.len()).unwrap_or(0),
                        length2: batch.as_ref().map(|b| b.len()).unwrap_or(0),
                        complete: None,
                    }
                ));
                let compaction_frontier = Some(self.logical_frontier.borrow());
                self.merging[index] = MergeState::begin_merge(old, batch, compaction_frontier);
            }
            MergeState::Double(_) => {
                panic!("Attempted to insert batch into incomplete merge!")
            }
        };
    }

    /// Completes and extracts what ever is at layer `index`.
    fn complete_at(&mut self, index: usize) -> Option<B> {
        if let Some((merged, inputs)) = self.merging[index].complete() {
            if let Some((input1, input2)) = inputs {
                // Log the completion of a merge from existing parts.
                self.logger.as_ref().map(|l| l.log(
                    ::logging::MergeEvent {
                        operator: self.operator.global_id,
                        scale: index,
                        length1: input1.len(),
                        length2: input2.len(),
                        complete: Some(merged.len()),
                    }
                ));
            }
            Some(merged)
        }
        else {
            None
        }
    }

    /// Attempts to draw down large layers to size appropriate layers.
    fn tidy_layers(&mut self) {

        // If the largest layer is complete (not merging), we can attempt
        // to draw it down to the next layer. This is permitted if we can
        // maintain our invariant that below each merge there are at most
        // half the records that would be required to invade the merge.
        if !self.merging.is_empty() {
            let mut length = self.merging.len();
            if self.merging[length-1].is_single() {

                // To move a batch down, we require that it contain few
                // enough records that the lower level is appropriate,
                // and that moving the batch would not create a merge
                // violating our invariant.

                let appropriate_level = self.merging[length-1].len().next_power_of_two().trailing_zeros() as usize;

                // Continue only as far as is appropriate
                while appropriate_level < length-1 {

                    match self.merging[length-2].take() {
                        // Vacant or structurally empty batches can be absorbed.
                        MergeState::Vacant | MergeState::Single(None) => {
                            self.merging.remove(length-2);
                            length = self.merging.len();
                        }
                        // Single batches may initiate a merge, if sizes are
                        // within bounds, but terminate the loop either way.
                        MergeState::Single(Some(batch)) => {

                            // Determine the number of records that might lead
                            // to a merge. Importantly, this is not the number
                            // of actual records, but the sum of upper bounds
                            // based on indices.
                            let mut smaller = 0;
                            for (index, batch) in self.merging[..(length-2)].iter().enumerate() {
                                match batch {
                                    MergeState::Vacant => { },
                                    MergeState::Single(_) => { smaller += 1 << index; },
                                    MergeState::Double(_) => { smaller += 2 << index; },
                                }
                            }

                            if smaller <= (1 << length) / 8 {
                                self.merging.remove(length-2);
                                self.insert_at(Some(batch), length-2);
                            }
                            else {
                                self.merging[length-2] = MergeState::Single(Some(batch));
                            }
                            return;
                        }
                        // If a merge is in progress there is nothing to do.
                        MergeState::Double(state) => {
                            self.merging[length-2] = MergeState::Double(state);
                            return;
                        }
                    }
                }
            }
        }
    }
}


/// Describes the state of a layer.
///
/// A layer can be empty, contain a single batch, or contain a pair of batches
/// that are in the process of merging into a batch for the next layer.
pub(crate) enum MergeState<B: Batch> {
    /// An empty layer, containing no updates.
    Vacant,
    /// A layer containing a single batch.
    ///
    /// The `None` variant is used to represent a structurally empty batch present
    /// to ensure the progress of maintenance work.
    Single(Option<B>),
    /// A layer containing two batches, in the process of merging.
    Double(MergeVariant<B>),
}

impl<B: Batch> MergeState<B> where B::Time: Eq {

    /// The number of actual updates contained in the level.
    fn len(&self) -> usize {
        match self {
            MergeState::Single(Some(b)) => b.len(),
            MergeState::Double(MergeVariant::InProgress(b1,b2,_)) => b1.len() + b2.len(),
            MergeState::Double(MergeVariant::Complete(Some((b, _)))) => b.len(),
            _ => 0,
        }
    }

    /// True only for the MergeState::Vacant variant.
    fn is_vacant(&self) -> bool {
        if let MergeState::Vacant = self { true } else { false }
    }

    /// True only for the MergeState::Single variant.
    fn is_single(&self) -> bool {
        if let MergeState::Single(_) = self { true } else { false }
    }

    /// True only for the MergeState::Double variant.
    fn is_double(&self) -> bool {
        if let MergeState::Double(_) = self { true } else { false }
    }

    /// Immediately complete any merge.
    ///
    /// The result is either a batch, if there is a non-trivial batch to return
    /// or `None` if there is no meaningful batch to return. This does not distinguish
    /// between Vacant entries and structurally empty batches, which should be done
    /// with the `is_complete()` method.
    ///
    /// There is the addional option of input batches.
    fn complete(&mut self) -> Option<(B, Option<(B, B)>)>  {
        match std::mem::replace(self, MergeState::Vacant) {
            MergeState::Vacant => None,
            MergeState::Single(batch) => batch.map(|b| (b, None)),
            MergeState::Double(variant) => variant.complete(),
        }
    }

    /// True iff the layer is a complete merge, ready for extraction.
    fn is_complete(&mut self) -> bool {
        if let MergeState::Double(MergeVariant::Complete(_)) = self {
            true
        }
        else {
            false
        }
    }

    /// Performs a bounded amount of work towards a merge.
    ///
    /// If the merge completes, the resulting batch is returned.
    /// If a batch is returned, it is the obligation of the caller
    /// to correctly install the result.
    fn work(&mut self, fuel: &mut isize) {
        // We only perform work for merges in progress.
        if let MergeState::Double(layer) = self {
            layer.work(fuel)
        }
    }

    /// Extract the merge state, typically temporarily.
    fn take(&mut self) -> Self {
        std::mem::replace(self, MergeState::Vacant)
    }

    /// Initiates the merge of an "old" batch with a "new" batch.
    ///
    /// The upper frontier of the old batch should match the lower
    /// frontier of the new batch, with the resulting batch describing
    /// their composed interval, from the lower frontier of the old
    /// batch to the upper frontier of the new batch.
    ///
    /// Either batch may be `None` which corresponds to a structurally
    /// empty batch whose upper and lower froniers are equal. This
    /// option exists purely for bookkeeping purposes, and no computation
    /// is performed to merge the two batches.
    fn begin_merge(batch1: Option<B>, batch2: Option<B>, compaction_frontier: Option<AntichainRef<B::Time>>) -> MergeState<B> {
        let variant =
        match (batch1, batch2) {
            (Some(batch1), Some(batch2)) => {
                assert!(batch1.upper() == batch2.lower());
                let begin_merge = <B as Batch>::begin_merge(&batch1, &batch2, compaction_frontier);
                MergeVariant::InProgress(batch1, batch2, begin_merge)
            }
            (None, Some(x)) => MergeVariant::Complete(Some((x, None))),
            (Some(x), None) => MergeVariant::Complete(Some((x, None))),
            (None, None) => MergeVariant::Complete(None),
        };

        MergeState::Double(variant)
    }
}

pub(crate) enum MergeVariant<B: Batch> {
    /// Describes an actual in-progress merge between two non-trivial batches.
    InProgress(B, B, <B as Batch>::Merger),
    /// A merge that requires no further work. May or may not represent a non-trivial batch.
    Complete(Option<(B, Option<(B, B)>)>),
}

impl<B: Batch> MergeVariant<B> {

    /// Completes and extracts the batch, unless structurally empty.
    ///
    /// The result is either `None`, for structurally empty batches,
    /// or a batch and optionally input batches from which it derived.
    fn complete(mut self) -> Option<(B, Option<(B, B)>)> {
        let mut fuel = isize::max_value();
        self.work(&mut fuel);
        if let MergeVariant::Complete(batch) = self { batch }
        else { panic!("Failed to complete a merge!"); }
    }

    /// Applies some amount of work, potentially completing the merge.
    ///
    /// In case the work completes, the source batches are returned.
    /// This allows the caller to manage the released resources.
    fn work(&mut self, fuel: &mut isize) {
        let variant = std::mem::replace(self, MergeVariant::Complete(None));
        if let MergeVariant::InProgress(b1,b2,mut merge) = variant {
            merge.work(&b1,&b2,fuel);
            if *fuel > 0 {
                *self = MergeVariant::Complete(Some((merge.done(), Some((b1,b2)))));
            }
            else {
                *self = MergeVariant::InProgress(b1,b2,merge);
            }
        }
        else {
            *self = variant;
        }
    }
}
