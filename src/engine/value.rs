// Copyright © 2024 Pathway

#![allow(clippy::non_canonical_partial_ord_impl)] // False positive with Derivative

use std::fmt::{self, Debug, Display};
use std::mem::{align_of, size_of};
use std::ops::Deref;
use std::sync::Arc;

use super::error::{DataError, DynError, DynResult};
use super::time::{DateTime, DateTimeNaive, DateTimeUtc, Duration};
use super::PyObjectWrapper;

use arcstr::ArcStr;
use cfg_if::cfg_if;
use derivative::Derivative;
use itertools::Itertools as _;
use ndarray::ArrayD;
use ordered_float::OrderedFloat;
use rand::Rng;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use xxhash_rust::xxh3::Xxh3 as Hasher;

pub const BASE32_ALPHABET: base32::Alphabet = base32::Alphabet::Crockford;

cfg_if! {
    if #[cfg(feature="yolo-id32")] {
        pub type KeyImpl = u32;
    } else if #[cfg(feature="yolo-id64")] {
        pub type KeyImpl = u64;
    } else {
        pub type KeyImpl = u128;
    }
}

pub const SHARD_MASK: KeyImpl = (1 << 16) - 1;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Key(pub KeyImpl);

impl Key {
    const FOR_EMPTY_TUPLE: Self = Self(0x40_10_8D_33_B7); // PWSRT42

    pub(crate) fn from_hasher(hasher: &Hasher) -> Self {
        Self(hasher.digest128() as KeyImpl)
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
        values.iter().for_each(|v| v.hash_into(&mut hasher));
        Self::from_hasher(&hasher)
    }

    pub fn random() -> Self {
        Self(rand::rng().random())
    }

    #[must_use]
    pub fn salted_with(self, seed: KeyImpl) -> Self {
        Self(self.0 ^ seed)
    }

    #[must_use]
    pub fn with_shard_of(self, other: Key) -> Self {
        Self((self.0 & (!SHARD_MASK)) | (other.0 & SHARD_MASK))
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let encoded = base32::encode(BASE32_ALPHABET, &self.0.to_le_bytes());
        write!(f, "^{encoded}")
    }
}

impl Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ShardPolicy {
    WholeKey,
    LastKeyColumn,
}

impl ShardPolicy {
    pub fn from_last_column_is_instance(last_column_is_instance: bool) -> Self {
        if last_column_is_instance {
            Self::LastKeyColumn
        } else {
            Self::WholeKey
        }
    }

    pub fn generate_key(&self, values: &[Value]) -> Key {
        match self {
            Self::WholeKey => Key::for_values(values),
            Self::LastKeyColumn => {
                Key::for_values(values).with_shard_of(Key::for_value(values.last().unwrap()))
            }
        }
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

fn serialize_json<S>(json: &JsonValue, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&json.to_string())
}

struct JsonVisitor;

impl Visitor<'_> for JsonVisitor {
    type Value = Handle<JsonValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("A String containing a serialized JSON.")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match from_str_safe(v) {
            Ok(json) => Ok(Handle::new(json)),
            Err(err) => Err(serde::de::Error::custom(err)),
        }
    }
}

fn deserialize_json<'de, D>(d: D) -> Result<Handle<JsonValue>, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_str(JsonVisitor)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Value {
    None,
    Bool(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    Pointer(Key),
    String(ArcStr),
    Bytes(Arc<[u8]>),
    Tuple(Arc<[Self]>),
    IntArray(Handle<ArrayD<i64>>),
    FloatArray(Handle<ArrayD<f64>>),
    DateTimeNaive(DateTimeNaive),
    DateTimeUtc(DateTimeUtc),
    Duration(Duration),
    #[serde(
        serialize_with = "serialize_json",
        deserialize_with = "deserialize_json"
    )]
    Json(Handle<JsonValue>),
    Error,
    PyObjectWrapper(Handle<PyObjectWrapper>),
    Pending,
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
        DynError::from(DataError::TypeMismatch {
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

    pub fn as_ordered_float(&self) -> DynResult<OrderedFloat<f64>> {
        if let Self::Float(f) = self {
            Ok(*f)
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

    pub fn as_json(&self) -> DynResult<&JsonValue> {
        if let Self::Json(json) = self {
            Ok(json)
        } else {
            Err(self.type_mismatch("Json"))
        }
    }

    pub fn into_result(self) -> DynResult<Self> {
        match self {
            Self::Error => Err(DataError::ErrorInValue.into()),
            value => Ok(value),
        }
    }
}

impl Display for Value {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::None => write!(fmt, "None"),
            Self::Bool(b) => write!(fmt, "{}", if *b { "True" } else { "False" }),
            Self::Int(i) => write!(fmt, "{i}"),
            Self::Float(OrderedFloat(f)) => write!(fmt, "{f:?}"),
            Self::Pointer(p) => write!(fmt, "{p}"),
            Self::String(s) => write!(fmt, "{s:?}"),
            Self::Bytes(b) => write!(fmt, "{b:?}"),
            Self::Tuple(vals) => write!(fmt, "({})", vals.iter().format(", ")),
            Self::IntArray(array) => write!(fmt, "{array}"),
            Self::FloatArray(array) => write!(fmt, "{array}"),
            Self::DateTimeNaive(date_time) => write!(fmt, "{date_time}"),
            Self::DateTimeUtc(date_time) => write!(fmt, "{date_time}"),
            Self::Duration(duration) => write!(fmt, "{duration}"),
            Self::Json(json) => write!(fmt, "{json}"),
            Self::Error => write!(fmt, "Error"),
            Self::PyObjectWrapper(ob) => write!(fmt, "{ob}"),
            Self::Pending => write!(fmt, "Pending"),
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

impl From<&[u8]> for Value {
    fn from(b: &[u8]) -> Self {
        Self::Bytes(b.into())
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

impl From<Vec<Value>> for Value {
    fn from(t: Vec<Value>) -> Self {
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

impl From<JsonValue> for Value {
    fn from(json: JsonValue) -> Self {
        Self::Json(Handle::new(json))
    }
}

impl From<PyObjectWrapper> for Value {
    fn from(ob: PyObjectWrapper) -> Self {
        Self::PyObjectWrapper(Handle::new(ob))
    }
}

// Please only append to this list, as the values here are used in hashing,
// so changing them will result in changed IDs
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Kind {
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
    Bytes,
    Json,
    Error,
    PyObjectWrapper,
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Any,
    Bool,
    Int,
    Float,
    Pointer,
    String,
    Bytes,
    DateTimeNaive,
    DateTimeUtc,
    Duration,
    Array(Option<usize>, Arc<Type>),
    Json,
    Tuple(Arc<[Type]>),
    List(Arc<Type>),
    PyObjectWrapper,
    Optional(Arc<Type>),
    Future(Arc<Type>),
}

impl Type {
    pub fn can_be_none(&self) -> bool {
        matches!(self, Self::Optional(_) | Self::Any)
    }

    pub fn unoptionalize(&self) -> &Self {
        match self {
            Self::Optional(arg) => arg,
            type_ => type_,
        }
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional(_))
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Any => write!(f, "Any"),
            Type::Bool => write!(f, "bool"),
            Type::Int => write!(f, "int"),
            Type::Float => write!(f, "float"),
            Type::Pointer => write!(f, "Pointer"),
            Type::String => write!(f, "str"),
            Type::Bytes => write!(f, "bytes"),
            Type::DateTimeNaive => write!(f, "DateTimeNaive"),
            Type::DateTimeUtc => write!(f, "DateTimeUtc"),
            Type::Duration => write!(f, "Duration"),
            Type::Array(dim, arg) => {
                if let Some(dim) = dim {
                    write!(f, "Array({dim}, {arg})")
                } else {
                    write!(f, "Array({arg})")
                }
            }
            Type::Json => write!(f, "Json"),
            Type::Tuple(args) => write!(f, "tuple[{}]", args.iter().format(", ")),
            Type::List(arg) => write!(f, "list[{arg}]"),
            Type::PyObjectWrapper => write!(f, "PyObjectWrapper"),
            Type::Optional(arg) => write!(f, "{arg} | None"),
            Type::Future(arg) => write!(f, "Future[{arg}]"),
        }
    }
}

impl Value {
    #[must_use]
    pub fn kind(&self) -> Kind {
        match self {
            Self::None => Kind::None,
            Self::Bool(_) => Kind::Bool,
            Self::Int(_) => Kind::Int,
            Self::Float(_) => Kind::Float,
            Self::Pointer(_) => Kind::Pointer,
            Self::String(_) => Kind::String,
            Self::Bytes(_) => Kind::Bytes,
            Self::Tuple(_) => Kind::Tuple,
            Self::IntArray(_) => Kind::IntArray,
            Self::FloatArray(_) => Kind::FloatArray,
            Self::DateTimeNaive(_) => Kind::DateTimeNaive,
            Self::DateTimeUtc(_) => Kind::DateTimeUtc,
            Self::Duration(_) => Kind::Duration,
            Self::Json(_) => Kind::Json,
            Self::Error => Kind::Error,
            Self::PyObjectWrapper(_) => Kind::PyObjectWrapper,
            Self::Pending => Kind::Pending,
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
        (self.kind() as u8).hash_into(hasher);
        match self {
            Self::None => {}
            Self::Bool(b) => b.hash_into(hasher),
            Self::Int(i) => i.hash_into(hasher),
            Self::Float(f) => f.hash_into(hasher),
            Self::Pointer(p) => p.hash_into(hasher),
            Self::String(s) => s.hash_into(hasher),
            Self::Bytes(b) => b.hash_into(hasher),
            Self::Tuple(vals) => vals.hash_into(hasher),
            Self::IntArray(handle) => handle.hash_into(hasher),
            Self::FloatArray(handle) => handle.hash_into(hasher),
            Self::DateTimeNaive(date_time) => date_time.hash_into(hasher),
            Self::DateTimeUtc(date_time) => date_time.hash_into(hasher),
            Self::Duration(duration) => duration.hash_into(hasher),
            Self::Json(json) => json.hash_into(hasher),
            Self::Error => panic!("trying to hash error"), // FIXME
            Self::PyObjectWrapper(ob) => ob.hash_into(hasher),
            Self::Pending => panic!("trying to hash pending"), // FIXME
        }
    }
}

impl HashInto for JsonValue {
    fn hash_into(&self, hasher: &mut Hasher) {
        (*self).to_string().hash_into(hasher);
    }
}

impl HashInto for PyObjectWrapper {
    fn hash_into(&self, hasher: &mut Hasher) {
        self.as_bytes()
            .expect("PyObjectWrapper serialization should not fail")
            .hash_into(hasher);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PointerParseError {
    #[error("the given value is not a base 32 string")]
    MalformedBase32String,

    #[error("the decoded array has a length different from 16")]
    IncorrectLength,
}

fn from_str_safe<'a, T>(s: &'a str) -> serde_json::Result<T>
where
    T: serde::de::Deserialize<'a>,
{
    let mut deserializer = serde_json::Deserializer::from_str(s);
    deserializer.disable_recursion_limit();
    let mut deserializer = serde_stacker::Deserializer::new(&mut deserializer);
    T::deserialize(&mut deserializer).map_err(|e| e.into_inner())
}

pub fn parse_pathway_pointer(serialized: &str) -> Result<Value, PointerParseError> {
    let encoded_pointer = &serialized[1..];
    let decoded = base32::decode(BASE32_ALPHABET, encoded_pointer)
        .ok_or(PointerParseError::MalformedBase32String)?;
    if decoded.len() == 16 {
        let decoded: [u8; 16] = decoded.try_into().unwrap();
        let key = KeyImpl::from_le_bytes(decoded);
        Ok(Value::Pointer(Key(key)))
    } else {
        Err(PointerParseError::IncorrectLength)
    }
}
