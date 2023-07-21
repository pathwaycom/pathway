#![allow(clippy::match_wildcard_for_single_variants)] // too many false positives

use std::cell::RefCell;
use std::collections::HashMap;
use std::iter::once;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::collection::concatenate;
use differential_dataflow::operators::{Iterate, JoinCore, Reduce};
use differential_dataflow::{AsCollection, Collection};
use itertools::zip_eq;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::Partition;

use super::maybe_total::MaybeTotalScope;
use super::operators::{ArrangeWithTypes, MapWrapped, MaybeTotal};
use super::shard::Shard;
use super::{ArrangedByKey, ArrangedBySelf, Column, DataflowGraphInner, UnwrapWithReporter};
use crate::engine::error::DynResult;
use crate::engine::{
    ColumnHandle, ComplexColumn, Computer, Context as ContextTrait, Error, Key, Result,
    UniverseHandle, Value,
};

#[derive(Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
struct Request {
    column_index: usize,
    key: Key,
    args: Option<Arc<[Value]>>,
}

impl Shard for Request {
    fn shard(&self) -> u64 {
        self.key.shard()
    }
}

#[derive(Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
enum Event {
    Request(Request),
    Reply(Request, Value),
    Dependency(Request, Request),
}

impl Shard for Event {
    fn shard(&self) -> u64 {
        match self {
            Self::Request(request) => request.shard(),
            Self::Reply(request, _reply) => request.shard(),
            Self::Dependency(_requestor, requested) => requested.shard(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
enum ComputerOutput {
    Reply(Value),
    Request(Request),
}

#[derive(Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
enum ComputerInput {
    // relative ordering of variants is important
    Request,
    RequestWithData(Value),
    Dependency(Request, Value),
}

enum ComputerLogic {
    Attribute(Box<dyn FnMut(&dyn ContextTrait) -> DynResult<Option<Value>>>),
    Method(Box<dyn FnMut(&dyn ContextTrait, &[Value]) -> DynResult<Option<Value>>>),
}

struct ComputerDetails {
    logic: ComputerLogic,
    data_column_handle: Option<ColumnHandle>,
}

impl ComputerLogic {
    fn compute(&mut self, context: &dyn ContextTrait, args: &[Value]) -> DynResult<Option<Value>> {
        match self {
            Self::Attribute(logic) => {
                assert!(args.is_empty());
                logic(context)
            }
            Self::Method(logic) => logic(context, args),
        }
    }
}

impl From<Computer> for ComputerDetails {
    fn from(computer: Computer) -> Self {
        match computer {
            Computer::Attribute { logic, .. } => Self {
                logic: ComputerLogic::Attribute(logic),
                data_column_handle: None,
            },
            Computer::Method {
                logic,
                data_column_handle,
                ..
            } => Self {
                logic: ComputerLogic::Method(logic),
                data_column_handle,
            },
        }
    }
}

struct Context<'a> {
    key: Key,
    data: Value,
    values: HashMap<&'a Request, &'a Value>,
    requests: RefCell<Vec<Request>>,
}

impl<'a> ContextTrait for Context<'a> {
    fn this_row(&self) -> Key {
        self.key
    }

    fn data(&self) -> Value {
        self.data.clone()
    }

    fn get(&self, column_index: usize, row: Key, args: Vec<Value>) -> Option<Value> {
        let args = if args.is_empty() {
            None
        } else {
            Some(Arc::from(args))
        };
        // XXX: verify index
        let request = Request {
            column_index,
            key: row,
            args,
        };
        if let Some(&value) = self.values.get(&request) {
            return Some(value.clone());
        }
        self.requests.borrow_mut().push(request);
        None
    }
}

#[allow(clippy::too_many_lines)] // XXX
pub(super) fn complex_columns<S: MaybeTotalScope>(
    graph: &mut DataflowGraphInner<S>,
    inputs: Vec<ComplexColumn>,
) -> Result<Vec<ColumnHandle>> {
    let mut input_details = Vec::new();
    let mut input_events = Vec::new();
    let mut computers = Vec::new();
    let mut outputs = Vec::new();
    for (column_index, complex_column) in inputs.into_iter().enumerate() {
        let InputParts {
            events,
            computer_details: computer_logic,
            output,
        } = handle_input(graph, complex_column, column_index)?;
        input_events.extend(events);
        let computer_index = computer_logic.map(|computer_logic| {
            let computer_index = computers.len();
            computers.push(computer_logic);
            computer_index
        });
        let output_index = output.map(|output| {
            let output_index = outputs.len();
            outputs.push(output);
            output_index
        });
        input_details.push(InputDetails {
            computer_index,
            output_index,
        });
    }
    let input_details: Rc<[InputDetails]> = input_details.into();
    let mut scope = graph.scope.clone();
    let input_events = concatenate(&mut scope, input_events);
    let events = input_events.iterate(|events| {
        let subscope = events.scope();
        let requests = events
            .flat_map(|event| match event {
                Event::Request(request) => Some(request),
                Event::Dependency(_requestor, requested) => Some(requested),
                _ => None,
            })
            .distinct();

        let dependencies: ArrangedByKey<_, Request, Request> = events
            .flat_map(|event| match event {
                Event::Dependency(requestor, requested) => Some((requestor, requested)),
                _ => None,
            })
            .arrange_named("dependencies");

        let values: ArrangedByKey<_, Request, Value> = events
            .flat_map(|event| match event {
                Event::Reply(request, value) => Some((request, value)),
                _ => None,
            })
            .arrange_named("values");

        let mut new_events = Vec::new();

        let input_details_ = input_details.clone();
        let computer_requests = requests
            .flat_map(move |request| {
                let index = input_details_[request.column_index].computer_index;
                index.map(|index| (u64::try_from(index).unwrap(), request))
            })
            .inner
            .partition(
                computers.len().try_into().unwrap(),
                |((index, inner_request), time, diff)| (index, (inner_request, time, diff)),
            )
            .into_iter()
            .map(|stream| stream.as_collection());

        zip_eq(computers, computer_requests).for_each(|(mut computer, our_requests)| {
            let our_request_inputs = match computer.data_column_handle {
                None => our_requests.map_named("complex_columns::our_requests None", |request| {
                    (request, ComputerInput::Request)
                }),
                Some(data_column_handle) => {
                    let data_column = &graph.columns[data_column_handle];
                    let our_requests_by_key: ArrangedByKey<_, Key, Request> = our_requests
                        .map_named("complex_columns::our_requests Some", |request| {
                            (request.key, request)
                        })
                        .arrange();
                    our_requests_by_key.join_core(
                        &data_column.values_arranged().enter(&subscope),
                        |_key, request, data| {
                            once((
                                request.clone(),
                                ComputerInput::RequestWithData(data.clone()),
                            ))
                        },
                    )
                }
            };
            let our_requests_arranged: ArrangedBySelf<_, Request> = our_requests.arrange();
            let reverse_dependencies_arranged: ArrangedByKey<_, Request, Request> = dependencies
                .join_core(&our_requests_arranged, |requestor, requested, ()| {
                    once((requested.clone(), requestor.clone()))
                })
                .arrange();
            let our_inputs = our_request_inputs.concat(&reverse_dependencies_arranged.join_core(
                &values,
                |requested, requestor, value| {
                    once((
                        requestor.clone(),
                        ComputerInput::Dependency(requested.clone(), value.clone()),
                    ))
                },
            ));
            let error_reporter = graph.error_reporter.clone();
            let our_outputs = our_inputs.reduce(move |request, input, output| {
                let (&(first, first_count), rest) = input.split_first().unwrap();
                assert_eq!(first_count, 1);
                let data = match first {
                    ComputerInput::Request => Value::None,
                    ComputerInput::RequestWithData(data) => data.clone(),
                    _ => {
                        // this should only happen when the result used to be needed
                        return;
                    }
                };
                let values = rest
                    .iter()
                    .map(|(input, count)| {
                        assert_eq!(count, &1);
                        match input {
                            ComputerInput::Dependency(requested, value) => (requested, value),
                            _ => unreachable!("expected only one request"),
                        }
                    })
                    .collect();
                let requests = RefCell::new(Vec::new());
                let context = Context {
                    key: request.key,
                    data,
                    values,
                    requests,
                };
                let result = computer
                    .logic
                    .compute(&context, request.args.as_deref().unwrap_or(&[]))
                    .unwrap_with_reporter(&error_reporter);
                if let Some(value) = result {
                    output.push((ComputerOutput::Reply(value), 1));
                }
                output.extend(
                    context
                        .requests
                        .into_inner()
                        .into_iter()
                        .map(|request| (ComputerOutput::Request(request), 1)),
                );
            });
            new_events.push(our_outputs.flat_map(|(request, output)| match output {
                ComputerOutput::Reply(value) => Some(Event::Reply(request, value)),
                ComputerOutput::Request(requested) => Some(Event::Dependency(request, requested)),
            }));
        });

        events.concatenate(new_events).distinct()
    });
    let column_handles = input_details
        .iter()
        .filter_map(|input| {
            input
                .output_index
                .map(|output_index| outputs[output_index].to_column_handle(graph, &events))
        })
        .collect();
    Ok(column_handles)
}

#[derive(Debug, Clone)]
enum Output {
    Attribute {
        column_index: usize,
        universe_handle: UniverseHandle,
    },
    Method {
        data: Value,
        universe_handle: UniverseHandle,
    },
}

impl Output {
    fn to_column_handle<S: MaybeTotalScope>(
        &self,
        graph: &mut DataflowGraphInner<S>,
        events: &Collection<S, Event>,
    ) -> ColumnHandle {
        match *self {
            Self::Attribute {
                column_index,
                universe_handle,
            } => {
                let universe = &graph.universes[universe_handle];
                let values = events.flat_map(move |event| match event {
                    // XXX partition
                    Event::Reply(request, value) => {
                        if request.column_index == column_index {
                            assert!(request.args.is_none());
                            Some((request.key, value))
                        } else {
                            None
                        }
                    }
                    _ => None,
                });
                graph.assert_keys_match_values(universe.keys(), &values);
                graph
                    .columns
                    .alloc(Column::from_collection(universe_handle, values))
            }
            Self::Method {
                ref data,
                universe_handle,
            } => {
                let data = data.clone();
                let universe = &graph.universes[universe_handle];
                let values = universe
                    .keys()
                    .map_named("to_column_handle::values", move |key| {
                        let value = Value::from([data.clone(), Value::from(key)].as_slice());
                        (key, value)
                    });
                graph
                    .columns
                    .alloc(Column::from_collection(universe_handle, values))
            }
        }
    }
}

struct InputParts<S: MaybeTotalScope> {
    events: Option<Collection<S, Event>>,
    computer_details: Option<ComputerDetails>,
    output: Option<Output>,
}

struct InputDetails {
    computer_index: Option<usize>,
    output_index: Option<usize>,
}

fn handle_input<S: MaybeTotalScope>(
    graph: &DataflowGraphInner<S>,
    complex_column: ComplexColumn,
    column_index: usize,
) -> Result<InputParts<S>> {
    let input = match complex_column {
        ComplexColumn::Column(column_handle) => {
            let column = graph
                .columns
                .get(column_handle)
                .ok_or(Error::InvalidColumnHandle)?;
            let events =
                column
                    .values()
                    .map_named("handle_input::Column::events", move |(key, value)| {
                        let request = Request {
                            column_index,
                            key,
                            args: None,
                        };
                        Event::Reply(request, value)
                    });
            InputParts {
                events: Some(events),
                computer_details: None,
                output: None,
            }
        }
        ComplexColumn::ExternalComputer(Computer::Attribute {
            logic,
            universe_handle,
        }) => {
            let universe = graph
                .universes
                .get(universe_handle)
                .ok_or(Error::InvalidUniverseHandle)?;
            let events =
                universe
                    .keys()
                    .map_named("handle_input::ExternalComputer::events", move |key| {
                        Event::Request(Request {
                            column_index,
                            key,
                            args: None,
                        })
                    });
            InputParts {
                events: Some(events),
                computer_details: Some(ComputerDetails {
                    logic: ComputerLogic::Attribute(logic),
                    data_column_handle: None,
                }),
                output: Some(Output::Attribute {
                    column_index,
                    universe_handle,
                }),
            }
        }
        ComplexColumn::ExternalComputer(Computer::Method {
            logic,
            universe_handle,
            data,
            data_column_handle,
        }) => {
            if let Some(data_column_handle) = data_column_handle {
                let data_column = graph
                    .columns
                    .get(data_column_handle)
                    .ok_or(Error::InvalidColumnHandle)?;
                if data_column.universe != universe_handle {
                    return Err(Error::UniverseMismatch);
                }
            }
            InputParts {
                events: None,
                computer_details: Some(ComputerDetails {
                    logic: ComputerLogic::Method(logic),
                    data_column_handle,
                }),
                output: Some(Output::Method {
                    data,
                    universe_handle,
                }),
            }
        }
        ComplexColumn::InternalComputer(computer) => InputParts {
            events: None,
            computer_details: Some(computer.into()),
            output: None,
        },
    };
    Ok(input)
}
