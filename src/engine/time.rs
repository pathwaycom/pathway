use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use chrono::{self, DurationRound, LocalResult, TimeZone};
use chrono::{Datelike, Timelike};
use chrono_tz::Tz;
use num_integer::Integer;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::{Error, Result};

#[allow(clippy::module_name_repetitions)]
pub trait DateTime {
    fn timestamp(&self) -> i64;

    fn as_chrono_datetime(&self) -> chrono::NaiveDateTime {
        let timestamp = self.timestamp();
        let (mut secs, mut nanos) = (timestamp / 1_000_000_000, timestamp % 1_000_000_000);
        if nanos < 0 {
            secs -= 1;
            nanos += 1_000_000_000;
        }
        chrono::NaiveDateTime::from_timestamp_opt(secs, u32::try_from(nanos).unwrap()).unwrap()
    }

    fn nanosecond(&self) -> i64 {
        self.as_chrono_datetime().nanosecond().into()
    }

    fn microsecond(&self) -> i64 {
        (self.as_chrono_datetime().nanosecond() / 1_000).into()
    }

    fn millisecond(&self) -> i64 {
        (self.as_chrono_datetime().nanosecond() / 1_000_000).into()
    }

    fn second(&self) -> i64 {
        self.as_chrono_datetime().second().into()
    }

    fn minute(&self) -> i64 {
        self.as_chrono_datetime().minute().into()
    }

    fn hour(&self) -> i64 {
        self.as_chrono_datetime().hour().into()
    }

    fn day(&self) -> i64 {
        self.as_chrono_datetime().day().into()
    }

    fn month(&self) -> i64 {
        self.as_chrono_datetime().month().into()
    }

    fn year(&self) -> i64 {
        self.as_chrono_datetime().year().into()
    }

    fn strftime(&self, format: &str) -> String;

    fn get_rounded_timestamp(&self, duration: Duration) -> i64 {
        self.as_chrono_datetime()
            .duration_round(duration.as_chrono_duration())
            .unwrap()
            .timestamp_nanos()
    }

    fn get_truncated_timestamp(&self, duration: Duration) -> i64 {
        self.as_chrono_datetime()
            .duration_trunc(duration.as_chrono_duration())
            .unwrap()
            .timestamp_nanos()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateTimeNaive {
    timestamp: i64,
}

impl DateTimeNaive {
    pub fn new(timestamp: i64) -> Self {
        Self { timestamp }
    }

    pub fn strptime(date_string: &str, format: &str) -> Result<Self> {
        if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(date_string, format) {
            Ok(Self {
                timestamp: datetime.timestamp_nanos(),
            })
        } else if let Ok(date) = chrono::NaiveDate::parse_from_str(date_string, format) {
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
            Ok(Self {
                timestamp: datetime.timestamp_nanos(),
            })
        } else if let Ok(time) = chrono::NaiveTime::parse_from_str(date_string, format) {
            let datetime = chrono::Utc::now().date_naive().and_time(time);
            Ok(Self {
                timestamp: datetime.timestamp_nanos(),
            })
        } else {
            Err(Error::ParseError(format!(
                "Cannot format date: {date_string} using format: {format}."
            )))
        }
    }

    pub fn to_utc_from_timezone(&self, timezone: &str) -> Result<DateTimeUtc> {
        if let Ok(tz) = timezone.parse::<Tz>() {
            let naive_local = self.as_chrono_datetime();
            let localized = tz.from_local_datetime(&naive_local);
            match localized {
                LocalResult::Single(localized) | LocalResult::Ambiguous(_, localized) => {
                    Ok(DateTimeUtc::new(localized.naive_utc().timestamp_nanos()))
                }
                LocalResult::None => {
                    // This NaiveDateTime doesn't exist in a given timezone.
                    // We try getting a first date after this.
                    let moved = naive_local + chrono::Duration::minutes(30);
                    let rounded = moved.duration_round(chrono::Duration::hours(1)).unwrap();
                    let localized = tz.from_local_datetime(&rounded);
                    if let LocalResult::Single(localized) = localized {
                        Ok(DateTimeUtc::new(localized.naive_utc().timestamp_nanos()))
                    } else {
                        Err(Error::DateTimeConversionError)
                    }
                }
            }
        } else {
            Err(Error::ParseError(format!(
                "Cannot parse time zone: {timezone}."
            )))
        }
    }

    #[must_use]
    pub fn round(&self, duration: Duration) -> DateTimeNaive {
        Self::new(self.get_rounded_timestamp(duration))
    }

    #[must_use]
    pub fn truncate(&self, duration: Duration) -> DateTimeNaive {
        Self::new(self.get_truncated_timestamp(duration))
    }

    pub fn from_timestamp(timestamp: i64, unit: &str) -> Result<Self> {
        let mult = match unit {
            "s" => Ok(1_000_000_000),
            "ms" => Ok(1_000_000),
            "us" => Ok(1_000),
            "ns" => Ok(1),
            _ => Err(Error::ValueError(format!(
                "unit has to be one of s, ms, us, ns but is {unit}."
            ))),
        }?;
        Ok(Self::new(mult * timestamp))
    }
}

impl DateTime for DateTimeNaive {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    fn strftime(&self, format: &str) -> String {
        self.as_chrono_datetime().format(format).to_string()
    }
}

impl Sub for DateTimeNaive {
    type Output = Duration;

    fn sub(self, other: Self) -> Self::Output {
        Duration {
            duration: self.timestamp - other.timestamp,
        }
    }
}

impl Add<Duration> for DateTimeNaive {
    type Output = Self;

    fn add(self, other: Duration) -> Self::Output {
        DateTimeNaive {
            timestamp: self.timestamp + other.duration,
        }
    }
}

impl Sub<Duration> for DateTimeNaive {
    type Output = Self;

    fn sub(self, other: Duration) -> Self::Output {
        DateTimeNaive {
            timestamp: self.timestamp - other.duration,
        }
    }
}

impl Display for DateTimeNaive {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.strftime("%Y-%m-%dT%H:%M:%S%.f"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateTimeUtc {
    timestamp: i64,
}

impl DateTimeUtc {
    pub fn new(timestamp: i64) -> Self {
        Self { timestamp }
    }

    pub fn strptime(date_string: &str, format: &str) -> Result<Self> {
        match chrono::DateTime::parse_from_str(date_string, format) {
            Ok(datetime) => Ok(Self {
                timestamp: datetime.timestamp_nanos(),
            }),
            Err(_) => Err(Error::ParseError(format!(
                "Cannot format date: {date_string} using format: {format}."
            ))),
        }
    }

    pub fn to_naive_in_timezone(&self, timezone: &str) -> Result<DateTimeNaive> {
        if let Ok(tz) = timezone.parse::<Tz>() {
            let naive_utc = self.as_chrono_datetime();
            let localized = tz.from_utc_datetime(&naive_utc);
            let naive_local = localized.naive_local();
            Ok(DateTimeNaive::new(naive_local.timestamp_nanos()))
        } else {
            Err(Error::ParseError(format!(
                "Cannot parse time zone: {timezone}."
            )))
        }
    }

    #[must_use]
    pub fn round(&self, duration: Duration) -> DateTimeUtc {
        Self::new(self.get_rounded_timestamp(duration))
    }

    #[must_use]
    pub fn truncate(&self, duration: Duration) -> DateTimeUtc {
        Self::new(self.get_truncated_timestamp(duration))
    }
}

impl DateTime for DateTimeUtc {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    fn strftime(&self, format: &str) -> String {
        chrono::Utc
            .timestamp_nanos(self.timestamp)
            .format(format)
            .to_string()
    }
}

impl Sub for DateTimeUtc {
    type Output = Duration;

    fn sub(self, other: Self) -> Self::Output {
        Duration {
            duration: self.timestamp - other.timestamp,
        }
    }
}

impl Add<Duration> for DateTimeUtc {
    type Output = Self;

    fn add(self, other: Duration) -> Self::Output {
        DateTimeUtc {
            timestamp: self.timestamp + other.duration,
        }
    }
}

impl Sub<Duration> for DateTimeUtc {
    type Output = Self;

    fn sub(self, other: Duration) -> Self::Output {
        DateTimeUtc {
            timestamp: self.timestamp - other.duration,
        }
    }
}

impl Display for DateTimeUtc {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.strftime("%Y-%m-%dT%H:%M:%S%.f%z"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Duration {
    duration: i64,
}

impl Duration {
    pub fn new(duration: i64) -> Self {
        Self { duration }
    }

    fn as_chrono_duration(self) -> chrono::Duration {
        chrono::Duration::nanoseconds(self.duration)
    }

    pub fn nanoseconds(&self) -> i64 {
        self.as_chrono_duration().num_nanoseconds().unwrap()
    }

    pub fn microseconds(&self) -> i64 {
        self.as_chrono_duration().num_microseconds().unwrap()
    }

    pub fn milliseconds(&self) -> i64 {
        self.as_chrono_duration().num_milliseconds()
    }

    pub fn seconds(&self) -> i64 {
        self.as_chrono_duration().num_seconds()
    }

    pub fn minutes(&self) -> i64 {
        self.as_chrono_duration().num_minutes()
    }

    pub fn hours(&self) -> i64 {
        self.as_chrono_duration().num_hours()
    }

    pub fn days(&self) -> i64 {
        self.as_chrono_duration().num_days()
    }

    pub fn weeks(&self) -> i64 {
        self.as_chrono_duration().num_weeks()
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn true_div(self, other: Self) -> Result<f64> {
        if other.duration == 0 {
            Err(Error::DivisionByZero)
        } else {
            Ok(self.duration as f64 / other.duration as f64)
        }
    }
}

impl Neg for Duration {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Duration {
            duration: -self.duration,
        }
    }
}

impl Add for Duration {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Duration {
            duration: self.duration + other.duration,
        }
    }
}

impl Sub for Duration {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Duration {
            duration: self.duration - other.duration,
        }
    }
}

impl Mul<i64> for Duration {
    type Output = Self;

    fn mul(self, other: i64) -> Self::Output {
        Duration {
            duration: self.duration * other,
        }
    }
}

impl Div for Duration {
    type Output = Result<i64>;

    fn div(self, other: Self) -> Self::Output {
        if other.duration == 0 {
            Err(Error::DivisionByZero)
        } else {
            Ok(Integer::div_floor(&self.duration, &other.duration))
        }
    }
}

impl Div<i64> for Duration {
    type Output = Result<Duration>;

    fn div(self, other: i64) -> Self::Output {
        if other == 0 {
            Err(Error::DivisionByZero)
        } else {
            Ok(Duration {
                duration: Integer::div_floor(&self.duration, &other),
            })
        }
    }
}

impl Rem for Duration {
    type Output = Result<Duration>;

    fn rem(self, other: Self) -> Self::Output {
        if other.duration == 0 {
            Err(Error::DivisionByZero)
        } else {
            Ok(Duration {
                duration: Integer::mod_floor(&self.duration, &other.duration),
            })
        }
    }
}

impl Display for Duration {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let timeunits = vec![
            (1_000_000_000 * 60 * 60 * 24, "d"),
            (1_000_000_000 * 60 * 60, "h"),
            (1_000_000_000 * 60, "m"),
            (1_000_000_000, "s"),
            (1, "ns"),
        ];
        let mut output = vec![];
        let mut remaining_nanoseconds = self.duration;
        for (num_nanoseconds, unit_name) in timeunits {
            if remaining_nanoseconds / num_nanoseconds != 0 {
                output.push(format!(
                    "{}{}",
                    remaining_nanoseconds / num_nanoseconds,
                    unit_name
                ));
                remaining_nanoseconds %= num_nanoseconds;
            }
        }
        write!(fmt, "{}", output.join(" "))
    }
}
