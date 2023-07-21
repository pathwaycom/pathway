use crate::engine::Key;

pub trait Shard {
    fn shard(&self) -> u64;

    #[allow(clippy::cast_possible_truncation)]
    fn shard_as_usize(&self) -> usize {
        self.shard() as usize
    }
}

impl Shard for Key {
    #[allow(clippy::cast_possible_truncation)]
    fn shard(&self) -> u64 {
        self.0 as u64
    }
}

impl<T> Shard for (Key, T) {
    fn shard(&self) -> u64 {
        self.0.shard()
    }
}

impl<T, U> Shard for (Key, T, U) {
    fn shard(&self) -> u64 {
        self.0.shard()
    }
}

impl Shard for i32 {
    #[allow(clippy::cast_sign_loss)]
    fn shard(&self) -> u64 {
        *self as u64
    }
}
