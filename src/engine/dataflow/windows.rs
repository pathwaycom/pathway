use std::sync::Arc;

use num_integer::Integer;

use crate::engine::{
    dataflow::Table, report_error::UnwrapWithReporter, ColumnPath, DateTimeNaive, DateTimeUtc,
    Duration, Error, Key, Result, TableHandle, TableProperties, Value, WindowProperties,
};

use super::{maybe_total::MaybeTotalScope, DataflowGraphInner};

trait ProduceWindows {
    fn produce_windows(&self, key: &Value) -> Result<Vec<(Value, Value)>>;
}

macro_rules! impl_windows_producer {
    ($name:ident, $cast_to:ty) => {
        impl ProduceWindows for $name {
            // The window multiplier `k` is an `i64`; casting it to `f64` for float-typed
            // windows is intentional and the loss of precision is acceptable here.
            #[allow(clippy::cast_precision_loss)]
            fn produce_windows(&self, key: &Value) -> Result<Vec<(Value, Value)>> {
                let key = Self::extract_key(&key)?;
                let last_k = Self::divide(key - self.origin, self.hop) + 1;
                let ratio = match self.ratio_or_duration {
                    RatioOrDuration::Ratio(ratio) => ratio,
                    RatioOrDuration::Duration(duration) => Self::divide(duration, self.hop),
                };
                let first_k = last_k - ratio - 2;
                let result = (first_k..=last_k)
                    .map(|k| {
                        let start = self.hop * (k as $cast_to) + self.origin;
                        let end = match self.ratio_or_duration {
                            RatioOrDuration::Ratio(ratio) => {
                                self.hop * ((k + ratio) as $cast_to) + self.origin
                            }
                            RatioOrDuration::Duration(duration) => {
                                self.hop * (k as $cast_to) + duration + self.origin
                            }
                        };
                        (start, end)
                    })
                    .filter(|(start, end)| start <= &key && &key < end)
                    .map(|(start, end)| (start.into(), end.into()))
                    .collect();
                Ok(result)
            }
        }
    };
}

enum RatioOrDuration<T> {
    Ratio(i64),
    Duration(T),
}

impl<T> RatioOrDuration<T> {
    fn new(ratio: Option<i64>, duration: Option<T>) -> Result<Self> {
        match (ratio, duration) {
            (Some(ratio), None) => Ok(Self::Ratio(ratio)),
            (None, Some(duration)) => Ok(Self::Duration(duration)),
            _ => Err(Error::IncorrectWindowParameter),
        }
    }
}

struct WindowPropertiesI64 {
    hop: i64,
    ratio_or_duration: RatioOrDuration<i64>,
    origin: i64,
}
impl WindowPropertiesI64 {
    fn divide(a: i64, b: i64) -> i64 {
        Integer::div_floor(&a, &b)
    }
    fn extract_key(key: &Value) -> Result<i64> {
        Ok(key.as_int()?)
    }
}
impl_windows_producer!(WindowPropertiesI64, i64);

struct WindowPropertiesF64 {
    hop: f64,
    ratio_or_duration: RatioOrDuration<f64>,
    origin: f64,
}
impl WindowPropertiesF64 {
    #[allow(clippy::cast_possible_truncation)]
    fn divide(a: f64, b: f64) -> i64 {
        (a / b).floor() as i64 //FIXME check if proper range
    }
    fn extract_key(key: &Value) -> Result<f64> {
        Ok(key.as_float()?)
    }
}
impl_windows_producer!(WindowPropertiesF64, f64);

struct WindowPropertiesDateTimeNaive {
    hop: Duration,
    ratio_or_duration: RatioOrDuration<Duration>,
    origin: DateTimeNaive,
}
impl WindowPropertiesDateTimeNaive {
    fn divide(a: Duration, b: Duration) -> i64 {
        a / b
    }
    fn extract_key(key: &Value) -> Result<DateTimeNaive> {
        Ok(key.as_date_time_naive()?)
    }
}
impl_windows_producer!(WindowPropertiesDateTimeNaive, i64);

struct WindowPropertiesDateTimeUtc {
    hop: Duration,
    ratio_or_duration: RatioOrDuration<Duration>,
    origin: DateTimeUtc,
}
impl WindowPropertiesDateTimeUtc {
    fn divide(a: Duration, b: Duration) -> i64 {
        a / b
    }
    fn extract_key(key: &Value) -> Result<DateTimeUtc> {
        Ok(key.as_date_time_utc()?)
    }
}
impl_windows_producer!(WindowPropertiesDateTimeUtc, i64);

fn create_window_producer(prop: WindowProperties) -> Result<Box<dyn ProduceWindows>> {
    match prop.origin {
        Value::Int(origin) => Ok(Box::new(WindowPropertiesI64 {
            hop: prop.hop.as_int()?,
            ratio_or_duration: RatioOrDuration::new(
                prop.ratio.map(|r| r.as_int()).transpose()?,
                prop.duration.map(|d| d.as_int()).transpose()?,
            )?,
            origin,
        })),
        Value::Float(origin) => Ok(Box::new(WindowPropertiesF64 {
            hop: prop.hop.as_float()?,
            ratio_or_duration: RatioOrDuration::new(
                prop.ratio.map(|r| r.as_int()).transpose()?,
                prop.duration.map(|d| d.as_float()).transpose()?,
            )?,
            origin: *origin,
        })),
        Value::DateTimeNaive(origin) => Ok(Box::new(WindowPropertiesDateTimeNaive {
            hop: prop.hop.as_duration()?,
            ratio_or_duration: RatioOrDuration::new(
                prop.ratio.map(|r| r.as_int()).transpose()?,
                prop.duration.map(|d| d.as_duration()).transpose()?,
            )?,
            origin,
        })),
        Value::DateTimeUtc(origin) => Ok(Box::new(WindowPropertiesDateTimeUtc {
            hop: prop.hop.as_duration()?,
            ratio_or_duration: RatioOrDuration::new(
                prop.ratio.map(|r| r.as_int()).transpose()?,
                prop.duration.map(|d| d.as_duration()).transpose()?,
            )?,
            origin,
        })),
        _ => Err(Error::IncorrectWindowParameter),
    }
}

pub fn assign_windows<S>(
    graph: &mut DataflowGraphInner<S>,
    table_handle: TableHandle,
    key_column_path: ColumnPath,
    window_properties: WindowProperties,
    table_properties: Arc<TableProperties>,
) -> Result<TableHandle>
where
    S: MaybeTotalScope,
{
    let table = graph
        .tables
        .get(table_handle)
        .ok_or(Error::InvalidTableHandle)?;

    let error_reporter = graph.error_reporter.clone();

    let window_producer = create_window_producer(window_properties)?;

    let new_values = table.values().flat_map(move |(id, values)| {
        let key = key_column_path
            .extract(&id, &values)
            .unwrap_with_reporter(&error_reporter);
        let windows = window_producer
            .produce_windows(&key)
            .unwrap_with_reporter(&error_reporter);
        windows
            .into_iter()
            .enumerate()
            .map(move |(i, (start, end))| {
                (
                    Key::for_values(&[Value::from(id), Value::from(i64::try_from(i).unwrap())]),
                    Value::from([values.clone(), start, end].as_slice()),
                )
            })
    });

    Ok(graph
        .tables
        .alloc(Table::from_collection(new_values).with_properties(table_properties)))
}
