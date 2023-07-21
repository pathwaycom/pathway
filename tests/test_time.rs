use pathway_engine::engine::Duration;

#[test]
fn test_duration_1() -> eyre::Result<()> {
    let d = Duration::new(93784987654321);
    assert_eq!(d.to_string(), "1d 2h 3m 4s 987654321ns");
    let d = Duration::new(-93784987654321);
    assert_eq!(d.to_string(), "-1d -2h -3m -4s -987654321ns");
    Ok(())
}

#[test]
fn test_duration_2() -> eyre::Result<()> {
    let d = Duration::new(2);
    assert_eq!(d.to_string(), "2ns");
    let d = Duration::new(-2);
    assert_eq!(d.to_string(), "-2ns");
    Ok(())
}

#[test]
fn test_duration_skip_units() -> eyre::Result<()> {
    let d = Duration::new(86400987654321);
    assert_eq!(d.to_string(), "1d 987654321ns");
    let d = Duration::new(-86400987654321);
    assert_eq!(d.to_string(), "-1d -987654321ns");
    Ok(())
}

#[test]
fn test_duration_zero_sec() -> eyre::Result<()> {
    let d = Duration::new(1197780000000000);
    assert_eq!(d.to_string(), "13d 20h 43m");
    let d = Duration::new(-1197780000000000);
    assert_eq!(d.to_string(), "-13d -20h -43m");
    Ok(())
}
