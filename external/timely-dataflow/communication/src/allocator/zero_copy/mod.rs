//! Allocators based on serialized data which avoid copies.
//!
//! These allocators are based on `Abomonation` serialization, and its ability to deserialized
//! typed Rust data in-place. They surface references to data, often ultimately referencing the
//! raw binary data they initial received.

pub mod allocator;
pub mod allocator_process;
pub mod bytes_exchange;
pub mod bytes_slab;
pub mod initialize;
pub mod push_pull;
pub mod stream;
pub mod tcp;
