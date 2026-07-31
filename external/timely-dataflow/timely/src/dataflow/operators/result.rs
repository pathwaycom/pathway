//! Extension methods for `Stream` containing `Result`s.

use crate::dataflow::operators::Map;
use crate::dataflow::{Scope, Stream};
use crate::Data;

/// Extension trait for `Stream`.
pub trait ResultStream<S: Scope, T: Data, E: Data> {
    /// Returns a new instance of `self` containing only `ok` records.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, ResultStream};
    ///
    /// timely::example(|scope| {
    ///     vec![Ok(0), Err(())].to_stream(scope)
    ///            .ok()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn ok(&self) -> Stream<S, T>;

    /// Returns a new instance of `self` containing only `err` records.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, ResultStream};
    ///
    /// timely::example(|scope| {
    ///     vec![Ok(0), Err(())].to_stream(scope)
    ///            .err()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn err(&self) -> Stream<S, E>;

    /// Returns a new instance of `self` applying `logic` on all `Ok` records.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, ResultStream};
    ///
    /// timely::example(|scope| {
    ///     vec![Ok(0), Err(())].to_stream(scope)
    ///            .map_ok(|x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_ok<T2: Data, L: FnMut(T) -> T2 + 'static>(&self, logic: L) -> Stream<S, Result<T2, E>>;

    /// Returns a new instance of `self` applying `logic` on all `Err` records.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, ResultStream};
    ///
    /// timely::example(|scope| {
    ///     vec![Ok(0), Err(())].to_stream(scope)
    ///            .map_err(|_| 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_err<E2: Data, L: FnMut(E) -> E2 + 'static>(&self, logic: L) -> Stream<S, Result<T, E2>>;

    /// Returns a new instance of `self` applying `logic` on all `Ok` records, passes through `Err`
    /// records.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, ResultStream};
    ///
    /// timely::example(|scope| {
    ///     vec![Ok(0), Err(())].to_stream(scope)
    ///            .and_then(|x| Ok(1 + 1))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn and_then<T2: Data, L: FnMut(T) -> Result<T2, E> + 'static>(
        &self,
        logic: L,
    ) -> Stream<S, Result<T2, E>>;

    /// Returns a new instance of `self` applying `logic` on all `Ok` records.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, ResultStream};
    ///
    /// timely::example(|scope| {
    ///     vec![Ok(1), Err(())].to_stream(scope)
    ///            .unwrap_or_else(|_| 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn unwrap_or_else<L: FnMut(E) -> T + 'static>(&self, logic: L) -> Stream<S, T>;
}

impl<S: Scope, T: Data, E: Data> ResultStream<S, T, E> for Stream<S, Result<T, E>> {
    fn ok(&self) -> Stream<S, T> {
        self.flat_map(Result::ok)
    }

    fn err(&self) -> Stream<S, E> {
        self.flat_map(Result::err)
    }

    fn map_ok<T2: Data, L: FnMut(T) -> T2 + 'static>(
        &self,
        mut logic: L,
    ) -> Stream<S, Result<T2, E>> {
        self.map(move |r| r.map(|x| logic(x)))
    }

    fn map_err<E2: Data, L: FnMut(E) -> E2 + 'static>(
        &self,
        mut logic: L,
    ) -> Stream<S, Result<T, E2>> {
        self.map(move |r| r.map_err(|x| logic(x)))
    }

    fn and_then<T2: Data, L: FnMut(T) -> Result<T2, E> + 'static>(
        &self,
        mut logic: L,
    ) -> Stream<S, Result<T2, E>> {
        self.map(move |r| r.and_then(|x| logic(x)))
    }

    fn unwrap_or_else<L: FnMut(E) -> T + 'static>(&self, mut logic: L) -> Stream<S, T> {
        self.map(move |r| r.unwrap_or_else(|err| logic(err)))
    }
}

#[cfg(test)]
mod tests {
    use crate::dataflow::operators::{capture::Extract, Capture, ResultStream, ToStream};

    #[test]
    fn test_ok() {
        let output = crate::example(|scope| vec![Ok(0), Err(())].to_stream(scope).ok().capture());
        assert_eq!(output.extract()[0].1, vec![0]);
    }

    #[test]
    fn test_err() {
        let output = crate::example(|scope| vec![Ok(0), Err(())].to_stream(scope).err().capture());
        assert_eq!(output.extract()[0].1, vec![()]);
    }

    #[test]
    fn test_map_ok() {
        let output = crate::example(|scope| {
            vec![Ok(0), Err(())]
                .to_stream(scope)
                .map_ok(|_| 10)
                .capture()
        });
        assert_eq!(output.extract()[0].1, vec![Ok(10), Err(())]);
    }

    #[test]
    fn test_map_err() {
        let output = crate::example(|scope| {
            vec![Ok(0), Err(())]
                .to_stream(scope)
                .map_err(|_| 10)
                .capture()
        });
        assert_eq!(output.extract()[0].1, vec![Ok(0), Err(10)]);
    }

    #[test]
    fn test_and_then() {
        let output = crate::example(|scope| {
            vec![Ok(0), Err(())]
                .to_stream(scope)
                .and_then(|_| Ok(1))
                .capture()
        });
        assert_eq!(output.extract()[0].1, vec![Ok(1), Err(())]);
    }

    #[test]
    fn test_unwrap_or_else() {
        let output = crate::example(|scope| {
            vec![Ok(0), Err(())]
                .to_stream(scope)
                .unwrap_or_else(|_| 10)
                .capture()
        });
        assert_eq!(output.extract()[0].1, vec![0, 10]);
    }
}
