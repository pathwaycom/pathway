# Copyright © 2024 Pathway


from pathway.internals import parse_graph
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.runtime_type_check import check_arg_types
from pathway.persistence import Config as PersistenceConfig


@check_arg_types
def run(
    *,
    debug: bool = False,
    monitoring_level: MonitoringLevel = MonitoringLevel.AUTO,
    with_http_server: bool = False,
    default_logging: bool = True,
    persistence_config: PersistenceConfig | None = None,
    runtime_typechecking: bool | None = None,
    terminate_on_error: bool | None = None,
) -> None:
    """Runs the computation graph.

    Args:
        debug: enable output out of table.debug() operators
        monitoring_level: the verbosity of stats monitoring mechanism. One of
            pathway.MonitoringLevel.NONE, pathway.MonitoringLevel.IN_OUT,
            pathway.MonitoringLevel.ALL. If unset, pathway will choose between
            NONE and IN_OUT based on output interactivity.
        with_http_server: whether to start a http server with runtime metrics. [will be deprecated soon]
            Learn more about Pathway monitoring in a
            `tutorial </developers/user-guide/deployment/pathway-monitoring/>`_ .
        default_logging: whether to allow pathway to set its own logging handler. Set
            it to False if you want to set your own logging handler.
        persistence_config: the config for persisting the state in case this
            persistence is required.
        runtime_typechecking: enables additional strict type checking at runtime
        terminate_on_error: whether to terminate the computation if the data/user-logic error occurs
    """
    GraphRunner(
        parse_graph.G,
        debug=debug,
        monitoring_level=monitoring_level,
        with_http_server=with_http_server,
        default_logging=default_logging,
        persistence_config=persistence_config,
        runtime_typechecking=runtime_typechecking,
        terminate_on_error=terminate_on_error,
        _stacklevel=4,
    ).run_outputs()


@check_arg_types
def run_all(
    *,
    debug: bool = False,
    monitoring_level: MonitoringLevel = MonitoringLevel.AUTO,
    with_http_server: bool = False,
    default_logging: bool = True,
    persistence_config: PersistenceConfig | None = None,
    runtime_typechecking: bool | None = None,
    terminate_on_error: bool | None = None,
) -> None:
    """Runs the computation graph with disabled tree-shaking optimization.

    Args:
        debug: enable output out of table.debug() operators
        monitoring_level: the verbosity of stats monitoring mechanism. One of
            pathway.MonitoringLevel.NONE, pathway.MonitoringLevel.IN_OUT,
            pathway.MonitoringLevel.ALL. If unset, pathway will choose between
            NONE and IN_OUT based on output interactivity.
        with_http_server: whether to start a http server with runtime metrics. [will be deprecated soon]
            Learn more about Pathway monitoring in a
            `tutorial </developers/user-guide/deployment/pathway-monitoring/>`_ .
        default_logging: whether to allow pathway to set its own logging handler. Set
            it to False if you want to set your own logging handler.
        persistence_config: the config for persisting the state in case this
            persistence is required.
        runtime_typechecking: enables additional strict type checking at runtime
        terminate_on_error: whether to terminate the computation if the data/user-logic error occurs
    """
    GraphRunner(
        parse_graph.G,
        debug=debug,
        monitoring_level=monitoring_level,
        with_http_server=with_http_server,
        default_logging=default_logging,
        persistence_config=persistence_config,
        runtime_typechecking=runtime_typechecking,
        terminate_on_error=terminate_on_error,
        _stacklevel=4,
    ).run_all()
