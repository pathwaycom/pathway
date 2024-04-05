import pickle
from collections import defaultdict, deque
from typing import Any

import networkx as nx
import numpy as np

import pathway as pw


def create_hmm_reducer(
    graph: nx.DiGraph, beam_size: int | None = None, num_results_kept: int | None = None
):
    """Generates a reducer performing decoding of the Hidden Markov Model.

    Args:
        graph: directed graph representing the state transitions of the HMM.
        beam_size: limits the search. Defaults to None
        num_results_kept: Maximum number of previous results returned. Defaults to None (keep all the results).

    Example:

    >>> import pathway as pw
    >>> import networkx as nx
    >>> from typing import Literal
    >>> from functools import partial
    >>> MANUL_STATES = Literal["HUNGRY", "FULL"]
    >>> MANUL_OBSERVATIONS = Literal["GRUMPY", "HAPPY"]
    >>> input_observations = pw.debug.table_from_markdown('''
    ...     observation | __time__
    ...      HAPPY      |     1
    ...      HAPPY      |     2
    ...      GRUMPY     |     3
    ...      GRUMPY     |     4
    ...      HAPPY      |     5
    ...      GRUMPY     |     6
    ... ''')
    >>> def get_manul_hmm() -> nx.DiGraph:
    ...    g = nx.DiGraph()
    ...    def _calc_emission_log_ppb(
    ...        observation: MANUL_OBSERVATIONS, state: MANUL_STATES
    ...    ) -> float:
    ...        if state == "HUNGRY":
    ...            if observation == "GRUMPY":
    ...                return np.log(0.9)
    ...            if observation == "HAPPY":
    ...                return np.log(0.1)
    ...        if state == "FULL":
    ...            if observation == "GRUMPY":
    ...                return np.log(0.7)
    ...            if observation == "HAPPY":
    ...                return np.log(0.3)
    ...    g.add_node(
    ...        "HUNGRY", calc_emission_log_ppb=partial(_calc_emission_log_ppb, state="HUNGRY")
    ...    )
    ...    g.add_node(
    ...        "FULL", calc_emission_log_ppb=partial(_calc_emission_log_ppb, state="FULL")
    ...    )
    ...    g.add_edge("HUNGRY", "HUNGRY", log_transition_ppb=np.log(0.4))
    ...    g.add_edge("HUNGRY", "FULL", log_transition_ppb=np.log(0.6))
    ...    g.add_edge("FULL", "HUNGRY", log_transition_ppb=np.log(0.6))
    ...    g.add_edge("FULL", "FULL", log_transition_ppb=np.log(0.4))
    ...    g.graph["start_nodes"] = ["HUNGRY", "FULL"]
    ...    return g
    >>> hmm_reducer = pw.reducers.udf_reducer(pw.stdlib.ml.hmm.create_hmm_reducer(get_manul_hmm(), num_results_kept=3))
    >>> decoded = input_observations.reduce(decoded_state=hmm_reducer(pw.this.observation))
    >>> pw.debug.compute_and_print_update_stream(decoded, include_id=False)
    decoded_state                | __time__ | __diff__
    ('FULL',)                    | 2        | 1
    ('FULL',)                    | 4        | -1
    ('FULL', 'FULL')             | 4        | 1
    ('FULL', 'FULL')             | 6        | -1
    ('FULL', 'FULL', 'HUNGRY')   | 6        | 1
    ('FULL', 'FULL', 'HUNGRY')   | 8        | -1
    ('FULL', 'HUNGRY', 'FULL')   | 8        | 1
    ('FULL', 'HUNGRY', 'FULL')   | 10       | -1
    ('HUNGRY', 'HUNGRY', 'FULL') | 10       | 1
    ('HUNGRY', 'HUNGRY', 'FULL') | 12       | -1
    ('HUNGRY', 'FULL', 'HUNGRY') | 12       | 1
    """
    # map graph nodes to consecutive integers
    idx_to_node = {}
    for i, node in enumerate(graph.nodes()):
        graph.nodes[node]["idx"] = i
        idx_to_node[i] = node

    class HmmAccumulator(pw.BaseCustomAccumulator):
        def __init__(self, observation):
            self.cnt = 1
            self.observation = observation

            self.ppb = np.full(graph.number_of_nodes(), -np.inf)
            self.backpointers: deque[np.ndarray] = deque([])

            # prepare starting nodes
            self.trimmed_nodes_idx = []
            for start_node in graph.graph["start_nodes"]:
                idx = graph.nodes[start_node]["idx"]
                self.ppb[idx] = graph.nodes[start_node]["calc_emission_log_ppb"](
                    observation
                )

                self.trimmed_nodes_idx.append(idx)

            if beam_size is None:
                self.beam_size = graph.number_of_nodes() + 1
            else:
                self.beam_size = beam_size
            self.path_states = tuple([idx_to_node[int(self.ppb.argmax())]])

        @classmethod
        def from_row(cls, row):
            [observation] = row
            return cls(observation)

        def update(self, other):
            assert other.cnt == 1
            self.cnt += other.cnt
            observation = other.observation
            new_backpointers = np.empty(graph.number_of_nodes(), dtype=int)

            reachable_nodes = defaultdict(list)
            new_ppb = np.full(graph.number_of_nodes(), -np.inf)

            for start_idx in self.trimmed_nodes_idx:
                start_node = idx_to_node[start_idx]
                start_node_cost = self.ppb[start_idx]
                for node in graph.successors(start_node):
                    log_transition_ppb = graph.get_edge_data(start_node, node)[
                        "log_transition_ppb"
                    ]
                    reachable_nodes[node].append(
                        (start_node_cost + log_transition_ppb, start_idx)
                    )
            trimmed_nodes_idx = []
            for node, candidates in reachable_nodes.items():
                node_emission_log_prob = graph.nodes[node]["calc_emission_log_ppb"](
                    observation
                )
                # if _hacky_speedups and node_emission_log_prob < -10000:
                # continue
                assert len(candidates) > 0, f"{node=}"
                max_ppb, max_idx = max(candidates)
                idx = graph.nodes[node]["idx"]
                new_ppb[idx] = node_emission_log_prob + max_ppb
                new_backpointers[idx] = max_idx
                trimmed_nodes_idx.append(idx)

            if len(trimmed_nodes_idx) > self.beam_size:
                trimmed_costs = new_ppb[trimmed_nodes_idx]
                sel = np.argpartition(
                    trimmed_costs, trimmed_costs.shape[0] - self.beam_size
                )
                trimmed_nodes_idx = [
                    trimmed_nodes_idx[s] for s in sel[-self.beam_size :]
                ]
            self.trimmed_nodes_idx = trimmed_nodes_idx
            self.backpointers.append(new_backpointers)
            best_final_state_idx = int(new_ppb.argmax())
            self.ppb = new_ppb
            path_idx = [best_final_state_idx]
            if (
                num_results_kept is not None
                and len(self.backpointers) >= num_results_kept
            ):
                self.backpointers.popleft()
            for backpointers in reversed(self.backpointers):
                path_idx.append(backpointers[path_idx[-1]])
            self.path_states = tuple(reversed([idx_to_node[x] for x in path_idx]))

        def compute_result(self) -> tuple[Any, ...]:
            return self.path_states

        def serialize(self):
            return pickle.dumps(
                (
                    self.cnt,
                    self.observation,
                    self.ppb,
                    self.backpointers,
                    self.trimmed_nodes_idx,
                    self.path_states,
                )
            )

        @classmethod
        def deserialize(cls, val):
            assert isinstance(val, bytes)
            (
                cnt,
                observation,
                ppb,
                backpointers,
                trimmed_nodes_idx,
                path_states,
            ) = pickle.loads(val)
            obj = cls(observation)
            obj.cnt = cnt
            obj.ppb = ppb
            obj.backpointers = backpointers
            obj.trimmed_nodes_idx = trimmed_nodes_idx
            obj.path_states = path_states
            return obj

        # implemented so we never actually process retracts,
        # but the resulting reducer uses less space
        def retract(self, other):
            raise NotImplementedError

    return HmmAccumulator
