use pathway_engine::engine::Key;

use pathway_engine::connectors::data_format::{Formatter, FormatterContext, NullFormatter};
use pathway_engine::connectors::data_storage::{NullWriter, Writer};

#[test]
fn test_null_formatter_ok() -> eyre::Result<()> {
    let mut formatter = NullFormatter::new();
    let key = Key::random();
    let context = formatter
        .format(&key, &Vec::new(), 1, 1)
        .expect("Formatter failed");

    assert_eq!(context.payloads, Vec::<Vec<u8>>::new());
    assert_eq!(context.key, key);
    assert_eq!(context.values, Vec::new());

    Ok(())
}

#[test]
fn test_null_writer_ok() -> eyre::Result<()> {
    let mut writer = NullWriter::new();
    let context =
        FormatterContext::new_single_payload(b"hello".to_vec(), Key::random(), Vec::new());
    writer.write(context).unwrap();
    Ok(())
}
