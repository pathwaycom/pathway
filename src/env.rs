use std::env;
use std::error;
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("couldn't parse the value of {0:?} environment variable as UTF-8 string")]
    NotUtf8(String),

    #[error("couldn't parse the value of {0:?} environment variable: {1}")]
    ParsingFailed(String, #[source] Box<dyn error::Error + Send + Sync>),

    #[error("environment variable {0:?} is not set")]
    NotSet(String),
}

pub fn parse_env_var<T: FromStr>(name: &str) -> Result<Option<T>, Error>
where
    T::Err: error::Error + Send + Sync + 'static,
{
    if let Some(value) = env::var_os(name) {
        Ok(Some(
            value
                .into_string()
                .map_err(|_| Error::NotUtf8(name.to_string()))?
                .parse()
                .map_err(|err| Error::ParsingFailed(name.to_string(), Box::new(err)))?,
        ))
    } else {
        Ok(None)
    }
}

pub fn parse_env_var_required<T: FromStr>(name: &str) -> Result<T, Error>
where
    T::Err: error::Error + Send + Sync + 'static,
{
    parse_env_var(name)?.ok_or_else(|| Error::NotSet(name.to_string()))
}
