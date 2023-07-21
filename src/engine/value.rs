use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt::{self, Display};
use std::mem::{align_of, size_of};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use arcstr::ArcStr;
use cfg_if::cfg_if;
use derivative::Derivative;
use itertools::Itertools as _;
use ndarray::ArrayD;
use ordered_float::OrderedFloat;
use rand::Rng;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use super::error::{DynError, DynResult};
use super::time::{DateTime, DateTimeNaive, DateTimeUtc, Duration};
use super::{Error, Result};

const BASE32_ALPHABET: base32::Alphabet = base32::Alphabet::Crockford;

cfg_if! {
    if #[cfg(feature="yolo-id32")] {
        pub type KeyImpl = u32;
    } else if #[cfg(feature="yolo-id64")] {
        pub type KeyImpl = u64;
    } else {
        pub type KeyImpl = u128;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Key(pub KeyImpl);

impl Key {
    const FOR_EMPTY_TUPLE: Self = Self(0x40_10_8D_33_B7); // PWSRT42

    pub(crate) fn from_hasher(hasher: &Hasher) -> Self {
        cfg_if! {
            if #[cfg(feature="strong-hash")] {
                let mut res = [0; KEY_BYTES];
                hasher.finalize_xof().fill(&mut res);
                Self(KeyImpl::from_le_bytes(res))
            } else {
                Self(hasher.digest128() as KeyImpl)
            }
        }
    }

    pub fn for_value(value: &Value) -> Self {
        let mut hasher = Hasher::default();
        value.hash_into(&mut hasher);
        Self::from_hasher(&hasher)
    }

    pub fn for_values(values: &[Value]) -> Self {
        if values.is_empty() {
            return Self::FOR_EMPTY_TUPLE;
        }
        let mut hasher = Hasher::default();
        values
            .iter()
            .for_each(|v| v.borrow().hash_into(&mut hasher));
        Self::from_hasher(&hasher)
    }

    pub fn random() -> Self {
        Self(rand::thread_rng().gen())
    }

    #[must_use]
    pub fn salted_with(self, seed: KeyImpl) -> Self {
        Self(self.0 ^ seed)
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.alternate() {
            write!(f, "^")?;
        }
        let encoded = base32::encode(BASE32_ALPHABET, &self.0.to_le_bytes());
        write!(f, "{encoded}")
    }
}

#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct HandleInner<T> {
    key: Key,

    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    data: T,
}

impl<T: HashInto> HandleInner<T> {
    pub fn new(inner: T) -> Self {
        let mut hasher = Hasher::default();
        inner.hash_into(&mut hasher);
        let key = Key::from_hasher(&hasher);
        Self { key, data: inner }
    }
}

#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(
    Clone(bound = ""),
    PartialEq(bound = ""),
    Eq(bound = ""),
    PartialOrd(bound = ""),
    Ord(bound = ""),
    Hash(bound = "")
)]
pub struct Handle<T>(Arc<HandleInner<T>>);

impl<T> Deref for Handle<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0.data
    }
}

impl<T: Display> Display for Handle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.data.fmt(f)
    }
}

impl<T: HashInto> Handle<T> {
    fn new(inner: T) -> Self {
        Self(Arc::new(HandleInner::new(inner)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Value {
    None,
    Bool(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    Pointer(Key),
    String(ArcStr),
    Tuple(Arc<[Self]>),
    IntArray(Handle<ArrayD<i64>>),
    FloatArray(Handle<ArrayD<f64>>),
    DateTimeNaive(DateTimeNaive),
    DateTimeUtc(DateTimeUtc),
    Duration(Duration),
}

const _: () = assert!(align_of::<Value>() <= 16);
const _: () = assert!(size_of::<Value>() <= 32);

impl Value {
    pub fn from_isize(i: isize) -> Self {
        match i.try_into() {
            Ok(i) => Self::Int(i),
            Err(_) => Self::None,
        }
    }

    #[inline(never)]
    #[cold]
    fn type_mismatch(&self, expected: &'static str) -> DynError {
        DynError::from(Error::TypeMismatch {
            expected,
            value: self.clone(),
        })
    }

    pub fn as_pointer(&self) -> DynResult<Key> {
        if let Value::Pointer(key) = self {
            Ok(*key)
        } else {
            Err(self.type_mismatch("pointer"))
        }
    }

    pub fn as_int(&self) -> DynResult<i64> {
        if let Value::Int(i) = self {
            Ok(*i)
        } else {
            Err(self.type_mismatch("integer"))
        }
    }

    pub fn as_bool(&self) -> DynResult<bool> {
        if let Value::Bool(b) = self {
            Ok(*b)
        } else {
            Err(self.type_mismatch("bool"))
        }
    }

    pub fn as_float(&self) -> DynResult<f64> {
        if let Self::Float(f) = self {
            Ok(f.into_inner())
        } else {
            Err(self.type_mismatch("float"))
        }
    }

    pub fn as_string(&self) -> DynResult<&ArcStr> {
        if let Self::String(s) = self {
            Ok(s)
        } else {
            Err(self.type_mismatch("string"))
        }
    }

    pub fn as_tuple(&self) -> DynResult<&Arc<[Self]>> {
        if let Self::Tuple(t) = self {
            Ok(t)
        } else {
            Err(self.type_mismatch("tuple"))
        }
    }

    pub fn as_date_time_naive(&self) -> DynResult<DateTimeNaive> {
        if let Self::DateTimeNaive(dt) = self {
            Ok(*dt)
        } else {
            Err(self.type_mismatch("DateTimeNaive"))
        }
    }

    pub fn as_date_time_utc(&self) -> DynResult<DateTimeUtc> {
        if let Self::DateTimeUtc(dt) = self {
            Ok(*dt)
        } else {
            Err(self.type_mismatch("DateTimeUtc"))
        }
    }

    pub fn as_duration(&self) -> DynResult<Duration> {
        if let Self::Duration(d) = self {
            Ok(*d)
        } else {
            Err(self.type_mismatch("Duration"))
        }
    }
}

impl Display for Value {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::None => write!(fmt, "none"),
            Self::Bool(b) => write!(fmt, "{}", if *b { "true" } else { "false" }),
            Self::Int(i) => write!(fmt, "{i}"),
            Self::Float(OrderedFloat(f)) => write!(fmt, "{f}"),
            Self::Pointer(p) => write!(fmt, "{p:#}"),
            Self::String(s) => write!(fmt, "\"{}\"", s.escape_default()),
            Self::Tuple(vals) => write!(fmt, "({})", vals.iter().format(", ")),
            Self::IntArray(array) => write!(fmt, "{array}"),
            Self::FloatArray(array) => write!(fmt, "{array}"),
            Self::DateTimeNaive(date_time) => write!(fmt, "{date_time}"),
            Self::DateTimeUtc(date_time) => write!(fmt, "{date_time}"),
            Self::Duration(duration) => write!(fmt, "{duration}"),
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self::Int(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Float(OrderedFloat(f))
    }
}

impl From<OrderedFloat<f64>> for Value {
    fn from(f: OrderedFloat<f64>) -> Self {
        Self::Float(f)
    }
}

impl From<Key> for Value {
    fn from(k: Key) -> Self {
        Self::Pointer(k)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::String(s.into())
    }
}

impl From<ArcStr> for Value {
    fn from(s: ArcStr) -> Self {
        Self::String(s)
    }
}

impl From<&[Value]> for Value {
    fn from(t: &[Value]) -> Self {
        Self::Tuple(t.into())
    }
}

impl From<ArrayD<i64>> for Value {
    fn from(a: ArrayD<i64>) -> Self {
        Self::IntArray(Handle::new(a))
    }
}

impl From<ArrayD<f64>> for Value {
    fn from(a: ArrayD<f64>) -> Self {
        Self::FloatArray(Handle::new(a))
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(o: Option<T>) -> Self {
        match o {
            None => Self::None,
            Some(v) => v.into(),
        }
    }
}

impl From<DateTimeNaive> for Value {
    fn from(dt: DateTimeNaive) -> Self {
        Self::DateTimeNaive(dt)
    }
}

impl From<DateTimeUtc> for Value {
    fn from(dt: DateTimeUtc) -> Self {
        Self::DateTimeUtc(dt)
    }
}

impl From<Duration> for Value {
    fn from(dt: Duration) -> Self {
        Self::Duration(dt)
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone)]
enum SimpleType {
    None,
    Bool,
    Int,
    Float,
    Pointer,
    String,
    Tuple,
    IntArray,
    FloatArray,
    DateTimeNaive,
    DateTimeUtc,
    Duration,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum Type {
    #[default]
    Any,
    Bool,
    Int,
    Float,
    String,
}

impl Value {
    #[must_use]
    fn simple_type(&self) -> SimpleType {
        match self {
            Self::None => SimpleType::None,
            Self::Bool(_) => SimpleType::Bool,
            Self::Int(_) => SimpleType::Int,
            Self::Float(_) => SimpleType::Float,
            Self::Pointer(_) => SimpleType::Pointer,
            Self::String(_) => SimpleType::String,
            Self::Tuple(_) => SimpleType::Tuple,
            Self::IntArray(_) => SimpleType::IntArray,
            Self::FloatArray(_) => SimpleType::FloatArray,
            Self::DateTimeNaive(_) => SimpleType::DateTimeNaive,
            Self::DateTimeUtc(_) => SimpleType::DateTimeUtc,
            Self::Duration(_) => SimpleType::Duration,
        }
    }
}

pub trait HashInto {
    fn hash_into(&self, hasher: &mut Hasher);
}

impl<T: HashInto> HashInto for &T {
    fn hash_into(&self, hasher: &mut Hasher) {
        (*self).hash_into(hasher);
    }
}

impl HashInto for f64 {
    fn hash_into(&self, hasher: &mut Hasher) {
        #[allow(clippy::float_cmp)]
        let raw = if self.is_nan() {
            !0
        } else if self == &0.0 {
            0 // -0.0 and 0.0 should hash to the same value
        } else {
            self.to_bits()
        };
        raw.hash_into(hasher);
    }
}

impl HashInto for OrderedFloat<f64> {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.0.hash_into(hasher);
    }
}

macro_rules! impl_hash_into_int {
    ($($type:path),+) => {
        $(impl HashInto for $type {
            fn hash_into(&self, hasher: &mut Hasher) {
                hasher.update(&self.to_le_bytes());
            }
        })+
    };
}

impl_hash_into_int!(i8, i16, i32, i64, i128);
impl_hash_into_int!(u8, u16, u32, u64, u128);

impl HashInto for usize {
    fn hash_into(&self, hasher: &mut Hasher) {
        u64::try_from(*self)
            .expect("usize fitting in 64 bits")
            .hash_into(hasher);
    }
}

impl HashInto for bool {
    fn hash_into(&self, hasher: &mut Hasher) {
        u8::from(*self).hash_into(hasher);
    }
}

impl<T> HashInto for HandleInner<T> {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.key.hash_into(hasher);
    }
}

impl HashInto for Key {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.0.hash_into(hasher);
    }
}

impl HashInto for str {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.len().hash_into(hasher);
        hasher.update(self.as_bytes());
    }
}

impl HashInto for String {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.as_str().hash_into(hasher);
    }
}

impl<T: HashInto> HashInto for [T] {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.len().hash_into(hasher);
        self.iter().for_each(|x| x.hash_into(hasher));
    }
}

impl<T: HashInto> HashInto for Vec<T> {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.as_slice().hash_into(hasher);
    }
}

impl<T: HashInto> HashInto for ArrayD<T> {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.shape().hash_into(hasher);
        self.iter().for_each(|x| x.hash_into(hasher));
    }
}

impl HashInto for DateTimeNaive {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.timestamp().hash_into(hasher);
    }
}

impl HashInto for DateTimeUtc {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.timestamp().hash_into(hasher);
    }
}

impl HashInto for Duration {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.nanoseconds().hash_into(hasher);
    }
}

impl HashInto for Value {
    fn hash_into(&self, hasher: &mut Hasher) {
        (self.simple_type() as u8).hash_into(hasher);
        match self {
            Self::None => {}
            Self::Bool(b) => b.hash_into(hasher),
            Self::Int(i) => i.hash_into(hasher),
            Self::Float(f) => f.hash_into(hasher),
            Self::Pointer(p) => p.hash_into(hasher),
            Self::String(s) => s.hash_into(hasher),
            Self::Tuple(vals) => vals.hash_into(hasher),
            Self::IntArray(handle) => handle.hash_into(hasher),
            Self::FloatArray(handle) => handle.hash_into(hasher),
            Self::DateTimeNaive(date_time) => date_time.hash_into(hasher),
            Self::DateTimeUtc(date_time) => date_time.hash_into(hasher),
            Self::Duration(duration) => duration.hash_into(hasher),
        }
    }
}

impl FromStr for Key {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = Value::from(s);
        Ok(Self::for_value(&v))
    }
}

impl FromStr for Value {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = Value::from(s);
        Ok(v)
    }
}
