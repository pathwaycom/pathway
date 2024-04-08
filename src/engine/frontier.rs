use serde::{Deserialize, Serialize};
use timely::order::TotalOrder;
use timely::progress::Antichain;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TotalFrontier<T> {
    At(T),
    Done,
}

impl<T: TotalOrder> From<Antichain<T>> for TotalFrontier<T> {
    fn from(value: Antichain<T>) -> Self {
        match value.into_option() {
            Some(t) => Self::At(t),
            None => Self::Done,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("total antichain can have at most one element")]
pub struct InvalidTotalAntichain;

impl<T: TotalOrder + Clone> TryFrom<&[T]> for TotalFrontier<T> {
    type Error = InvalidTotalAntichain;

    fn try_from(value: &[T]) -> Result<Self, Self::Error> {
        match value {
            [] => Ok(Self::Done),
            [t] => Ok(Self::At(t.clone())),
            _ => Err(InvalidTotalAntichain),
        }
    }
}

impl<T: TotalOrder> TotalFrontier<T> {
    pub fn is_time_done(&self, time: &T) -> bool {
        match self {
            Self::At(t) => time.less_than(t),
            Self::Done => true,
        }
    }

    pub fn is_time_pending(&self, time: &T) -> bool {
        !self.is_time_done(time)
    }

    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }
}
