//! Generic operators defined by user-provided closures.

pub mod builder_raw;
pub mod builder_rc;
pub mod operator;
// pub mod builder_ref;
mod handles;
mod notificator;
mod operator_info;

pub use self::handles::{
    FrontieredInputHandle, FrontieredInputHandleCore, InputHandle, InputHandleCore, OutputHandle,
    OutputHandleCore, OutputWrapper,
};
pub use self::notificator::{FrontierNotificator, Notificator};

pub use self::operator::{source, Operator};
pub use self::operator_info::OperatorInfo;
