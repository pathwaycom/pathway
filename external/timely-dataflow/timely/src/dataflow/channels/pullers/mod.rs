pub use self::counter::Counter;
pub mod counter;

// pub trait Pullable<T, D> {
//     fn pull(&mut self) -> Option<(&T, &mut Message<D>)>;
// }
//
// impl<T, D, P: ?Sized + Pullable<T, D>> Pullable<T, D> for Box<P> {
//     fn pull(&mut self) -> Option<(&T, &mut Message<D>)> { (**self).pull() }
// }
