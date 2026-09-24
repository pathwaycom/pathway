// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::sync::Arc;

use rdkafka::message::Headers;

use pathway_engine::connectors::data_format::{
    FormatterContext, MessageHeader, PathwayHeadersCache, PATHWAY_DIFF_HEADER, PATHWAY_TIME_HEADER,
};
use pathway_engine::engine::{Key, Timestamp, Value};

fn row(time: Timestamp, diff: isize) -> FormatterContext {
    FormatterContext::new_single_payload(
        b"payload".to_vec(),
        Key::random(),
        vec![
            Value::from("text"),
            Value::Bytes(Arc::from(b"\x00\xff".as_slice())),
            Value::None,
            Value::Int(42),
        ],
        time,
        diff,
    )
}

fn user_fields() -> Vec<(String, usize)> {
    vec![
        ("h_str".to_string(), 0),
        ("h_bytes".to_string(), 1),
        ("h_none".to_string(), 2),
        ("h_int".to_string(), 3),
    ]
}

fn owned<'a>(headers: impl Iterator<Item = MessageHeader<'a>>) -> Vec<(String, Option<Vec<u8>>)> {
    headers
        .map(|h| (h.key.to_string(), h.value.map(Cow::into_owned)))
        .collect()
}

#[test]
fn test_message_headers_follow_the_row_time_and_diff() {
    let fields = user_fields();
    let mut cache = PathwayHeadersCache::default();
    let rows = [
        row(Timestamp(10), 1),
        row(Timestamp(10), -1),
        row(Timestamp(10), -1),
        row(Timestamp(12), 1),
        row(Timestamp(12), 3),
        row(Timestamp(5), -2),
    ];
    for r in &rows {
        let headers = owned(r.message_headers(&fields, false, &mut cache));
        assert_eq!(
            headers,
            vec![
                (
                    PATHWAY_TIME_HEADER.to_string(),
                    Some(r.time.to_string().into_bytes())
                ),
                (
                    PATHWAY_DIFF_HEADER.to_string(),
                    Some(r.diff.to_string().into_bytes())
                ),
                ("h_str".to_string(), Some(b"text".to_vec())),
                ("h_bytes".to_string(), Some(b"\x00\xff".to_vec())),
                ("h_none".to_string(), None),
                ("h_int".to_string(), Some(b"42".to_vec())),
            ],
            "row with time {} and diff {}",
            r.time,
            r.diff
        );
    }
}

#[test]
fn test_message_headers_encode_bytes_on_request() {
    let fields = user_fields();
    let mut cache = PathwayHeadersCache::default();
    let r = row(Timestamp(7), 1);
    let headers = owned(r.message_headers(&fields, true, &mut cache));
    assert_eq!(headers[3], ("h_bytes".to_string(), Some(b"AP8=".to_vec())));
    assert_eq!(headers[2], ("h_str".to_string(), Some(b"text".to_vec())));
}

#[test]
fn test_kafka_headers_match_message_headers() {
    let fields = user_fields();
    let mut cache = PathwayHeadersCache::default();
    for r in [
        row(Timestamp(3), 1),
        row(Timestamp(3), -1),
        row(Timestamp(4), 1),
    ] {
        let kafka_headers = r.construct_kafka_headers(&fields, &mut cache);
        let expected = owned(r.message_headers(&fields, false, &mut cache));
        assert_eq!(kafka_headers.count(), expected.len());
        for (index, (key, value)) in expected.iter().enumerate() {
            let header = kafka_headers.get(index);
            assert_eq!(header.key, key);
            assert_eq!(header.value.map(<[u8]>::to_vec), *value);
        }
    }
}

#[test]
fn test_nats_headers_are_strings_with_none_spelled_out() {
    let fields = user_fields();
    let mut cache = PathwayHeadersCache::default();
    let r = row(Timestamp(9), -1);
    let nats_headers = r.construct_nats_headers(&fields, &mut cache);
    let get = |key: &str| nats_headers.get(key).map(|v| v.as_str().to_string());
    assert_eq!(get(PATHWAY_TIME_HEADER).as_deref(), Some("9"));
    assert_eq!(get(PATHWAY_DIFF_HEADER).as_deref(), Some("-1"));
    assert_eq!(get("h_str").as_deref(), Some("text"));
    assert_eq!(get("h_bytes").as_deref(), Some("AP8="));
    assert_eq!(get("h_none").as_deref(), Some("None"));
    assert_eq!(get("h_int").as_deref(), Some("42"));
}

#[test]
fn test_string_properties_serialize_user_values_as_json() {
    let fields = user_fields();
    let mut cache = PathwayHeadersCache::default();
    for r in [
        row(Timestamp(21), 1),
        row(Timestamp(21), -1),
        row(Timestamp(22), 1),
    ] {
        let properties = r.construct_string_properties(&fields, &mut cache);
        assert_eq!(
            properties,
            vec![
                (PATHWAY_TIME_HEADER.to_string(), r.time.to_string()),
                (PATHWAY_DIFF_HEADER.to_string(), r.diff.to_string()),
                ("h_str".to_string(), "\"text\"".to_string()),
                ("h_bytes".to_string(), "\"AP8=\"".to_string()),
                ("h_none".to_string(), "null".to_string()),
                ("h_int".to_string(), "42".to_string()),
            ]
        );
    }
}
