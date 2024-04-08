use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use differential_dataflow::input::InputSession;
use itertools::Itertools;
use scopeguard::guard;
use timely::dataflow::operators::{Inspect as _, Probe as _};
use timely::progress::Timestamp as _;

use crate::engine::report_error::ReportErrorExt as _;
use crate::engine::{
    ColumnPath, DataRow, Error, ExportedTable as ExportedTableTrait, ExportedTableCallback, Result,
    TableHandle, TableProperties, Timestamp, TotalFrontier, Value,
};

use super::maybe_total::MaybeTotalScope;
use super::{DataflowGraphInner, Table};

struct ExportedTable {
    failed: AtomicBool,
    properties: Arc<TableProperties>,
    frontier: Mutex<TotalFrontier<Timestamp>>,
    data: Mutex<Vec<DataRow>>,
    consumers: Mutex<Vec<Box<dyn FnMut() -> ControlFlow<()> + Send>>>,
}

impl ExportedTable {
    fn new(properties: Arc<TableProperties>) -> Self {
        let failed = AtomicBool::new(false);
        let frontier = Mutex::new(TotalFrontier::At(Timestamp::minimum()));
        let data = Mutex::new(Vec::new());
        let consumers = Mutex::new(Vec::new());

        Self {
            failed,
            properties,
            frontier,
            data,
            consumers,
        }
    }

    fn notify(&self) {
        self.consumers
            .lock()
            .unwrap()
            .retain_mut(|consumer| match consumer() {
                ControlFlow::Continue(()) => true,
                ControlFlow::Break(()) => false,
            });
    }

    fn push(&self, data: impl IntoIterator<Item = DataRow>) {
        self.data.lock().unwrap().extend(data);
        self.notify();
    }

    fn advance(&self, new_frontier: TotalFrontier<Timestamp>) {
        let must_notify = {
            let mut frontier = self.frontier.lock().unwrap();
            if new_frontier > *frontier {
                *frontier = new_frontier;
                true
            } else {
                assert_eq!(new_frontier, *frontier, "advancing the frontier backwards");
                false
            }
        };
        if must_notify {
            self.notify();
        }
    }

    fn mark_finished(&self) {
        if self.frontier().is_done() {
            return;
        }
        let already_failed = self.failed.swap(true, Ordering::Relaxed);
        if !already_failed {
            self.notify();
        }
    }
}

impl ExportedTableTrait for ExportedTable {
    fn failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    fn properties(&self) -> Arc<TableProperties> {
        self.properties.clone()
    }

    fn frontier(&self) -> TotalFrontier<Timestamp> {
        *self.frontier.lock().unwrap()
    }

    fn data_from_offset(&self, offset: usize) -> (Vec<DataRow>, usize) {
        let data = self.data.lock().unwrap();
        (data[offset..].to_vec(), data.len())
    }

    fn subscribe(&self, callback: ExportedTableCallback) {
        self.consumers.lock().unwrap().push(callback);
    }
}

pub fn export_table<S>(
    graph: &mut DataflowGraphInner<S>,
    table_handle: TableHandle,
    column_paths: Vec<ColumnPath>,
) -> Result<Arc<dyn ExportedTableTrait>>
where
    S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
{
    let table = graph
        .tables
        .get(table_handle)
        .ok_or_else(|| Error::InvalidTableHandle)?;
    let properties = Arc::new(TableProperties::Table(
        column_paths
            .iter()
            .map(|path| path.extract_properties(&table.properties))
            .try_collect()?,
    ));

    let exported_table = Arc::new(ExportedTable::new(properties));

    graph
        .extract_columns(table_handle, column_paths)?
        .as_collection()
        .inner
        .inspect_batch({
            let exported_table = exported_table.clone();
            move |_time, data| {
                exported_table.push(data.iter().map(|((key, values), time, diff)| {
                    DataRow::from_engine(*key, values.to_vec(), *time, *diff)
                }));
            }
        })
        .inspect_core({
            let exported_table = guard(exported_table.clone(), |exported_table| {
                exported_table.mark_finished();
            });
            move |event| {
                if let Err(frontier) = event {
                    exported_table.advance(frontier.try_into().unwrap());
                }
            }
        })
        .probe_with(&graph.output_probe);
    Ok(exported_table)
}

#[allow(clippy::unnecessary_wraps)] // we want to always return Result for symmetry
pub fn import_table<S>(
    graph: &mut DataflowGraphInner<S>,
    table: Arc<dyn ExportedTableTrait>,
) -> Result<TableHandle>
where
    S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
{
    let mut input_session = InputSession::new();
    let values = input_session.to_collection(&mut graph.scope);

    table.subscribe({
        let main_thread = thread::current();
        Box::new(move || {
            main_thread.unpark();
            ControlFlow::Continue(())
        })
    });
    graph.pollers.push({
        let mut offset = 0;
        let error_reporter = graph.error_reporter.clone();
        Box::new(move || {
            if table.failed() {
                error_reporter.report_and_panic(Error::ImportedTableFailed);
            }
            let frontier = table.frontier();
            let (data, new_offset) = table.data_from_offset(offset);
            for row in data {
                eprintln!("pushing {row:?}");
                input_session.update_at(
                    (row.key, Value::from(row.values.as_slice())),
                    row.time,
                    row.diff,
                );
            }
            offset = new_offset;
            match frontier {
                TotalFrontier::At(time) => {
                    input_session.advance_to(time);
                    input_session.flush();
                    ControlFlow::Continue(None)
                }
                TotalFrontier::Done => ControlFlow::Break(()),
            }
        })
    });
    let result = graph.tables.alloc(Table::from_collection(values));
    Ok(result)
}
