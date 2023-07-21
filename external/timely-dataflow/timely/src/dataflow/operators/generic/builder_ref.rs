//! Types to build operators with general shapes.

// TODO: Analogue to builder_rc.rs, which instead provides wrappers that only borrow ChangeBatch<T>
//       buffers, rather than wrapping them with Rc<RefCell<_>>. This removes dereferences from the
//       common path of having nothing to do, and could plausibly make it a bit faster to do nothing.
