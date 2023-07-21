#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![allow(clippy::must_use_candidate)] // too noisy

// FIXME:
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

pub mod connectors;
pub mod engine;
pub mod persistence;
pub mod python_api;

mod fs_helpers;
mod pipe;

#[cfg(not(feature = "standard-allocator"))]
mod jemalloc {
    use jemallocator::Jemalloc;

    #[global_allocator]
    static GLOBAL_ALLOCATOR: Jemalloc = Jemalloc;
}
