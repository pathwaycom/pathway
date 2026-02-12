// Copyright © 2026 Pathway

use std::collections::VecDeque;
use std::time::{Duration, Instant};

const DOWNSCALE_LOAD_SHARE_THRESHOLD: f64 = 0.25;

/// The progress in the computation is done with the repeated `step_or_park` calls.
/// Each `Event` contains information about a single call.
/// It has three principal durations:
/// - `step_duration`: how long the step lasted for.
/// - `compute_duration`: how long the computations lasted for, within this
///   step (always <= `step_duration`).
/// - `scheduled_duration`: how long it was allowed for the step to execute
///   (though can be < `step_duration` if the computation is overloaded).
struct Event {
    timestamp: Instant,
    step_duration: Duration,
    compute_duration: Duration,
    scheduled_duration: Duration,
}

#[derive(Debug, PartialEq)]
pub enum Advice {
    ScaleUp,
    ScaleDown,
    DoNothing,
}

pub struct WorkloadTracker {
    tracked_window: Duration,
    data_points: VecDeque<Event>,
    total_step_durations: Duration,
    total_compute_durations: Duration,
    total_scheduled_durations: Duration,
    has_full_window: bool,
}

impl WorkloadTracker {
    pub fn new(tracked_window: Duration) -> Self {
        Self {
            tracked_window,
            data_points: VecDeque::new(),
            total_step_durations: Duration::ZERO,
            total_compute_durations: Duration::ZERO,
            total_scheduled_durations: Duration::ZERO,
            has_full_window: false,
        }
    }

    pub fn add_point(
        &mut self,
        timestamp: Instant,
        step_duration: Duration,
        compute_duration: Duration,
        scheduled_duration: Duration,
    ) -> Advice {
        self.total_step_durations += step_duration;
        self.total_compute_durations += compute_duration;
        self.total_scheduled_durations += scheduled_duration;

        self.data_points.push_back(Event {
            timestamp,
            step_duration,
            compute_duration,
            scheduled_duration,
        });

        while let Some(earliest_event) = self.data_points.front() {
            if timestamp - earliest_event.timestamp <= self.tracked_window {
                break;
            }

            self.has_full_window = true;
            self.total_step_durations -= earliest_event.step_duration;
            self.total_compute_durations -= earliest_event.compute_duration;
            self.total_scheduled_durations -= earliest_event.scheduled_duration;
            self.data_points.pop_front();
        }

        self.advice()
    }

    fn advice(&self) -> Advice {
        if !self.has_full_window || self.total_step_durations == Duration::ZERO {
            return Advice::DoNothing;
        }

        // We want to scale up if the computational steps repeatedly take
        // more time than they were initially scheduled for.
        if self.total_compute_durations > self.total_scheduled_durations {
            return Advice::ScaleUp;
        }

        // We want to scale down, if the computation take relatively small
        // share of time spent, while most of the time the computational thread
        // is parked.
        if self.total_compute_durations.as_secs_f64()
            <= self.total_step_durations.as_secs_f64() * DOWNSCALE_LOAD_SHARE_THRESHOLD
        {
            Advice::ScaleDown
        } else {
            Advice::DoNothing
        }
    }
}
