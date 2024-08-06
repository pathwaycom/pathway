use std::ops::Deref;

use differential_dataflow::difference::Abelian;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::{AsCollection, Collection, Data};
use timely::dataflow::operators::generic::operator::empty;
use timely::progress::Timestamp;

use super::maybe_total::MaybeTotalScope;

// differential panics if `Variable` is dropped without being `set`
pub struct SafeVariable<S: MaybeTotalScope, D: Data, R: Abelian = isize> {
    scope: S,
    var: Option<Variable<S, D, R>>,
}

impl<S: MaybeTotalScope, D: Data, R: Abelian> SafeVariable<S, D, R> {
    pub fn new_from(
        source: Collection<S, D, R>,
        step: <S::Timestamp as Timestamp>::Summary,
    ) -> Self {
        Self {
            scope: source.scope(),
            var: Some(Variable::new_from(source, step)),
        }
    }

    pub fn set(mut self, result: &Collection<S, D, R>) {
        self.var.take().unwrap().set(result);
    }
}

impl<S: MaybeTotalScope, D: Data, R: Abelian> Deref for SafeVariable<S, D, R> {
    type Target = Collection<S, D, R>;

    #[allow(clippy::explicit_deref_methods)]
    fn deref(&self) -> &Self::Target {
        self.var.as_ref().unwrap().deref()
    }
}

impl<S: MaybeTotalScope, D: Data, R: Abelian> Drop for SafeVariable<S, D, R> {
    fn drop(&mut self) {
        self.var
            .take()
            .map(|var| var.set(&empty(&self.scope).as_collection()));
    }
}
