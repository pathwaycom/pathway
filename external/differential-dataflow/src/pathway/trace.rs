//! Bidirectional Trace - traits and implementation

use difference::Semigroup;
use lattice::Lattice;
use operators::arrange::TraceAgent;
use timely::progress::Antichain;
use trace::implementations::spine_fueled::{MergeState, MergeVariant, Spine};
use trace::{Batch, Cursor, Trace, TraceReader};

use std::fmt::Debug;

use pathway::cursor::BidirectionalCursor;
use timely::{progress::frontier::AntichainRef, PartialOrder};

use super::{batch::BidirectionalBatchReader, cursor::BidirectionalCursorList};
///Extends TraceReader by bidirectional cursor
pub trait BidirectionalTraceReader: TraceReader {
    /// type of bidirectional cursor
    type BidirectionalCursor: BidirectionalCursor<
        Key = Self::Key,
        Val = Self::Val,
        Time = Self::Time,
        R = Self::R,
    >;

    /// Acquires a bidirectional cursor to the restriction of the collection's contents to updates at times not greater or
    /// equal to an element of `upper`.
    ///
    /// This method is expected to work if called with an `upper` that (i) was an observed bound in batches from
    /// the trace, and (ii) the trace has not been advanced beyond `upper`. Practically, the implementation should
    /// be expected to look for a "clean cut" using `upper`, and if it finds such a cut can return a bidirectional cursor. This
    /// should allow `upper` such as `&[]` as used by `self.bidirectional_cursor()`, though it is difficult to imagine other uses.
    fn bidirectional_cursor_through(
        &mut self,
        upper: AntichainRef<Self::Time>,
    ) -> Option<(
        Self::BidirectionalCursor,
        <Self::BidirectionalCursor as Cursor>::Storage,
    )>;

    /// Provides a bidirectional cursor over updates contained in the trace.
    fn bidirectional_cursor(
        &mut self,
    ) -> (
        Self::BidirectionalCursor,
        <Self::BidirectionalCursor as Cursor>::Storage,
    ) {
        if let Some(cursor) = self.bidirectional_cursor_through(Antichain::new().borrow()) {
            cursor
        } else {
            panic!("unable to acquire complete cursor for trace; is it closed?");
        }
    }
}

/// A version of trace that provides bidirectional cursor through the values
pub trait NavigableTrace: Trace + BidirectionalTraceReader
where
    <Self as TraceReader>::Batch: Batch,
{
}

impl<B> BidirectionalTraceReader for Spine<B>
where
    B: BidirectionalBatchReader + Batch + Clone + 'static,
    B::Key: Ord + Clone, // Clone is required by `batch::advance_*` (in-place could remove).
    B::Val: Ord + Clone, // Clone is required by `batch::advance_*` (in-place could remove).
    B::Time: Lattice + timely::progress::Timestamp + Ord + Clone + Debug,
    B::R: Semigroup,
{
    type BidirectionalCursor =
        BidirectionalCursorList<<B as BidirectionalBatchReader>::BidirectionalCursor>;

    fn bidirectional_cursor_through(
        &mut self,
        upper: AntichainRef<Self::Time>,
    ) -> Option<(
        Self::BidirectionalCursor,
        <Self::BidirectionalCursor as Cursor>::Storage,
    )> {
        // If `upper` is the minimum frontier, we can return an empty cursor.
        // This can happen with operators that are written to expect the ability to acquire cursors
        // for their prior frontiers, and which start at `[T::minimum()]`, such as `Reduce`, sadly.
        if upper.less_equal(&<Self::Time as timely::progress::Timestamp>::minimum()) {
            let cursors = Vec::new();
            let storage = Vec::new();
            return Some((BidirectionalCursorList::new(cursors, &storage), storage));
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
        assert!(PartialOrder::less_equal(
            &self.physical_frontier.borrow(),
            &upper
        ));

        let mut cursors = Vec::new();
        let mut storage = Vec::new();

        for merge_state in self.merging.iter().rev() {
            match merge_state {
                MergeState::Double(variant) => match variant {
                    MergeVariant::InProgress(batch1, batch2, _) => {
                        if !batch1.is_empty() {
                            cursors.push(batch1.bidirectional_cursor());
                            storage.push(batch1.clone());
                        }
                        if !batch2.is_empty() {
                            cursors.push(batch2.bidirectional_cursor());
                            storage.push(batch2.clone());
                        }
                    }
                    MergeVariant::Complete(Some((batch, _))) => {
                        if !batch.is_empty() {
                            cursors.push(batch.bidirectional_cursor());
                            storage.push(batch.clone());
                        }
                    }
                    MergeVariant::Complete(None) => {}
                },
                MergeState::Single(Some(batch)) => {
                    if !batch.is_empty() {
                        cursors.push(batch.bidirectional_cursor());
                        storage.push(batch.clone());
                    }
                }
                MergeState::Single(None) => {}
                MergeState::Vacant => {}
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
                    cursors.push(batch.bidirectional_cursor());
                    storage.push(batch.clone());
                }
            }
        }

        Some((BidirectionalCursorList::new(cursors, &storage), storage))
    }
}

impl<Tr> BidirectionalTraceReader for TraceAgent<Tr>
where
    Tr: BidirectionalTraceReader,
    Tr::Time: Lattice + Ord + Clone + 'static,
{
    type BidirectionalCursor = Tr::BidirectionalCursor;
    fn bidirectional_cursor_through(
        &mut self,
        frontier: AntichainRef<Tr::Time>,
    ) -> Option<(
        Tr::BidirectionalCursor,
        <Tr::BidirectionalCursor as Cursor>::Storage,
    )> {
        self.trace
            .borrow_mut()
            .trace
            .bidirectional_cursor_through(frontier)
    }
}

/// Since Spine<B> is both trace and bidirectional trace reader it is a NavigableTrace as well.
impl<B> NavigableTrace for Spine<B>
where
    B: BidirectionalBatchReader + Batch + Clone + 'static,
    B::Key: Ord + Clone,
    B::Val: Ord + Clone,
    B::Time: Lattice + timely::progress::Timestamp + Ord + Clone + Debug,
    B::R: Semigroup,
{
}
