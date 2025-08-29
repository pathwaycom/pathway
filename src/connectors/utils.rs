use serde_json::Value as JsonValue;

pub fn from_str_safe<'a, T>(s: &'a str) -> serde_json::Result<T>
where
    T: serde::de::Deserialize<'a>,
{
    let mut deserializer = serde_json::Deserializer::from_str(s);
    deserializer.disable_recursion_limit();
    let mut deserializer = serde_stacker::Deserializer::new(&mut deserializer);
    T::deserialize(&mut deserializer).map_err(|e| e.into_inner())
}
