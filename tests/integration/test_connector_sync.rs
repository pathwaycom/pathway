// Copyright © 2024 Pathway

use assert_matches::assert_matches;
use futures::FutureExt;

use pathway_engine::connectors::data_format::ParsedEventWithErrors;
use pathway_engine::connectors::synchronization::{
    ConnectorGroupAccessor, ConnectorGroupDescriptor, ConnectorSynchronizer, Error,
};
use pathway_engine::engine::Value;

fn create_parsed_event(value: Value) -> ParsedEventWithErrors {
    ParsedEventWithErrors::Insert((None, vec![Ok(value)]))
}

fn start_two_equal_groups(
    group_1: &mut ConnectorGroupAccessor,
    group_2: &mut ConnectorGroupAccessor,
) -> eyre::Result<()> {
    // The first add wouldn't get an approval because it's unknown where
    // the second source starts from
    let first_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(0)));
    let mut sync_future = first_add.expect_wait();
    assert_matches!(sync_future.try_recv(), Ok(None));

    // Now the second source's start is known, so it gets an approval and
    // the future for the first add wakes up
    let second_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(1)));
    let approval = second_add.expect_approved();
    group_2.report_entries_sent(vec![approval]);
    assert!(sync_future.now_or_never().is_some());

    // The first add is retried and this time it gets an approval
    let first_add_retried = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(0)));
    let approval = first_add_retried.expect_approved();
    group_1.report_entries_sent(vec![approval]);

    Ok(())
}

#[test]
fn test_multiprocessed_runs_not_supported() -> eyre::Result<()> {
    let mut sync = ConnectorSynchronizer::new(true);
    let desc = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(10),
    };
    let group_add_result = sync.ensure_synchronization_group(&desc, 0);
    assert_matches!(group_add_result, Err(Error::MultiprocessingNotSupported));
    Ok(())
}

#[test]
fn test_incompatible_max_difference() -> eyre::Result<()> {
    let mut sync = ConnectorSynchronizer::new(false);
    let desc = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(10),
    };
    let group_add_result = sync.ensure_synchronization_group(&desc, 0);
    assert_matches!(group_add_result, Ok(_));

    let desc_2 = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(11),
    };
    let group_add_result = sync.ensure_synchronization_group(&desc_2, 1);

    assert_matches!(group_add_result, Err(Error::InconsistentWindowLength));
    Ok(())
}

#[test]
fn test_synchronization_simple() -> eyre::Result<()> {
    let mut sync = ConnectorSynchronizer::new(false);
    let desc = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(10),
    };
    let mut group_1 = sync.ensure_synchronization_group(&desc, 0).unwrap();
    let mut group_2 = sync.ensure_synchronization_group(&desc, 1).unwrap();

    start_two_equal_groups(&mut group_1, &mut group_2)?;

    // The third add is within the allowed interval, so it gets an immediate approval
    let third_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(2)));
    let approval = third_add.expect_approved();
    group_1.report_entries_sent(vec![approval]);

    // The fourth add and the fifth add are allowed, but not confirmed
    let fourth_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(10)));
    let approval_4 = fourth_add.expect_approved();
    let fifth_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(10)));
    let approval_5 = fifth_add.expect_approved();

    // Because of that, the thresholds are not yet updated. The sixth add would have
    // been confirmed, but now it's stuck since it waits for an actual advancements.
    let sixth_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(15)));
    let mut sync_future = sixth_add.expect_wait();
    assert_matches!(sync_future.try_recv(), Ok(None));

    // Now the forth and the fifth add are confirmed, so the sixth also gets its confirmation
    group_1.report_entries_sent(vec![approval_4, approval_5]);
    assert!(sync_future.now_or_never().is_some());

    // Checking that the sixth add passes now
    let sixth_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(15)));
    let approval = sixth_add.expect_approved();
    group_1.report_entries_sent(vec![approval]);

    Ok(())
}

#[test]
fn test_synchronization_jump_through_the_gap() -> eyre::Result<()> {
    let mut sync = ConnectorSynchronizer::new(false);
    let desc = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(10),
    };
    let mut group_1 = sync.ensure_synchronization_group(&desc, 0).unwrap();
    let mut group_2 = sync.ensure_synchronization_group(&desc, 1).unwrap();
    start_two_equal_groups(&mut group_1, &mut group_2)?;

    // The first source jumps far ahead of the interval, so it can't advance right now
    let third_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let mut sync_future = third_add.expect_wait();
    assert_matches!(sync_future.try_recv(), Ok(None));

    // The second source also jumps far ahead, therefore allowing the first source to advance
    let fourth_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let approval = fourth_add.expect_approved();
    group_2.report_entries_sent(vec![approval]);
    assert!(sync_future.now_or_never().is_some());

    // Now the first source is retried with the same value. The retry passes
    let third_add_retried = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let approval = third_add_retried.expect_approved();
    group_1.report_entries_sent(vec![approval]);

    Ok(())
}

#[test]
fn test_synchronization_wait_for_advancement() -> eyre::Result<()> {
    let mut sync = ConnectorSynchronizer::new(false);
    let desc = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(10),
    };
    let mut group_1 = sync.ensure_synchronization_group(&desc, 0).unwrap();
    let mut group_2 = sync.ensure_synchronization_group(&desc, 1).unwrap();
    start_two_equal_groups(&mut group_1, &mut group_2)?;

    // The first source jumps far ahead of the interval, so it can't advance right now
    let third_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let mut sync_future = third_add.expect_wait();
    assert_matches!(sync_future.try_recv(), Ok(None));

    // The second source advances by little, so that the first source can't yet retry
    let fourth_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(50)));
    let approval = fourth_add.expect_approved();
    group_2.report_entries_sent(vec![approval]);
    assert_matches!(sync_future.try_recv(), Ok(None));

    // The second source advances again, and still no possibility for the first source
    // to send its' event
    let fifth_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(80)));
    let approval = fifth_add.expect_approved();
    group_2.report_entries_sent(vec![approval]);
    assert_matches!(sync_future.try_recv(), Ok(None));

    // The second source finally catches up with the first source and get a pass
    let sixth_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let approval = sixth_add.expect_approved();
    group_2.report_entries_sent(vec![approval]);
    assert!(sync_future.now_or_never().is_some());

    // Now the first source is retried with the same value. The retry passes
    let third_add_retried = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let approval = third_add_retried.expect_approved();
    group_1.report_entries_sent(vec![approval]);

    Ok(())
}

#[test]
fn test_synchronization_several_workers() -> eyre::Result<()> {
    let mut sync = ConnectorSynchronizer::new(false);
    let desc = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(10),
    };
    let mut group_1 = sync.ensure_synchronization_group(&desc, 0).unwrap();
    let mut group_2 = sync.ensure_synchronization_group(&desc, 1).unwrap();
    let group_1_repeated = sync.ensure_synchronization_group(&desc, 0).unwrap();

    // The `source_id` is private, but `Debug` trait can be used to compare them
    assert_eq!(format!("{:?}", group_1), format!("{:?}", group_1_repeated));

    start_two_equal_groups(&mut group_1, &mut group_2)?;

    // The first source jumps far ahead of the interval, so it can't advance right now
    let third_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let mut sync_future = third_add.expect_wait();
    assert_matches!(sync_future.try_recv(), Ok(None));

    // The first source tries again with a smaller value, but still too big to advance
    let fourth_add = group_1_repeated.can_entry_be_sent(&create_parsed_event(Value::Int(15)));
    let mut sync_future = fourth_add.expect_wait();
    assert_matches!(sync_future.try_recv(), Ok(None));

    // Now the second source advances a little bit further, and the first source can advance
    let fifth_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(8)));
    let approval = fifth_add.expect_approved();
    group_2.report_entries_sent(vec![approval]);
    assert!(sync_future.now_or_never().is_some());

    // But this advancement was only done up to min{15, 100}, therefore the entry
    // for the bigger value still can't be sent
    let sixth_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(100)));
    let mut sync_future = sixth_add.expect_wait();
    assert_matches!(sync_future.try_recv(), Ok(None));

    Ok(())
}

#[test]
fn test_synchronization_several_workers_2() -> eyre::Result<()> {
    let mut sync = ConnectorSynchronizer::new(false);
    let desc = ConnectorGroupDescriptor {
        name: "default".to_string(),
        column_index: 0,
        max_difference: Value::Int(10),
    };
    let mut group_1 = sync.ensure_synchronization_group(&desc, 0).unwrap();
    let mut group_2 = sync.ensure_synchronization_group(&desc, 1).unwrap();

    // The first source proposes a value: 50. It has to wait because we don't know
    // what the second source will start from
    let first_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(50)));
    let mut sync_future_50 = first_add.expect_wait();
    assert_matches!(sync_future_50.try_recv(), Ok(None));

    // The first source proposes another value, a little bit smaller: 10. Still nothing
    // is known from the second source, so we wait
    let second_add = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(10)));
    let mut sync_future_10 = second_add.expect_wait();
    assert_matches!(sync_future_50.try_recv(), Ok(None));
    assert_matches!(sync_future_10.try_recv(), Ok(None));

    // Now the second source proposes a value: also 10. It succeeds, and the first source
    // can now also send its enqueued value of 10
    let third_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(10)));
    let approval = third_add.expect_approved();
    group_2.report_entries_sent(vec![approval]);
    assert!(sync_future_10.now_or_never().is_some());

    // Retry the ask for 10, get an approval, and send it. The future for 50 wakes up,
    // the re-request is done, but without success yet.
    let second_add_retried = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(10)));
    let approval = second_add_retried.expect_approved();
    group_1.report_entries_sent(vec![approval]);
    assert!(sync_future_50.now_or_never().is_some());
    let first_add_retry = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(50)));
    let mut sync_future_50 = first_add_retry.expect_wait();
    assert_matches!(sync_future_50.try_recv(), Ok(None));

    // Now the first source waits for approval for value 50
    // The second source sends a value of 200, which allows for two source to jump
    // the gap between 10 (current value) and 50 (the next proposed value)
    let fourth_add = group_2.can_entry_be_sent(&create_parsed_event(Value::Int(200)));
    let mut sync_future_200 = fourth_add.expect_wait();
    assert_matches!(sync_future_200.try_recv(), Ok(None));
    assert!(sync_future_50.now_or_never().is_some());

    // Retry request for sending 50 for the first source, and get an approval
    let first_add_retried = group_1.can_entry_be_sent(&create_parsed_event(Value::Int(50)));
    let approval = first_add_retried.expect_approved();
    group_1.report_entries_sent(vec![approval]);

    // But the second group still can't advance to 200: what if the first group will have entries
    // that are small enough to pass, but not big enough to move the threshold up to 200
    assert!(sync_future_200.now_or_never().is_none());

    Ok(())
}
