"""Core graph module for opensre.

Provides the base graph structure for defining and executing SRE workflows
as directed acyclic graphs (DAGs) of steps/nodes.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class GraphNode:
    """Represents a single step/node in an SRE workflow graph.

    Each node wraps a callable step function and tracks its dependencies
    and downstream connections within the graph.
    """

    def __init__(
        self,
        name: str,
        fn: Callable[..., Any],
        description: str = "",
    ) -> None:
        self.name = name
        self.fn = fn
        self.description = description
        self.upstream: List[str] = []
        self.downstream: List[str] = []

    def run(self, context: Dict[str, Any]) -> Any:
        """Execute the node's function with the current workflow context."""
        logger.debug("Running node: %s", self.name)
        return self.fn(context)

    def __repr__(self) -> str:
        return f"GraphNode(name={self.name!r})"


class Graph:
    """Directed acyclic graph representing an SRE workflow.

    Nodes are registered steps; edges define execution order.
    Execution is topologically sorted so dependencies always run first.

    Example usage::

        g = Graph(name="incident-response")

        @g.node(depends_on=[])
        def detect(ctx):
            ctx["alert"] = fetch_alert()

        @g.node(depends_on=["detect"])
        def triage(ctx):
            ctx["severity"] = classify(ctx["alert"])

        g.execute()
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._nodes: Dict[str, GraphNode] = {}
        self._edges: Dict[str, List[str]] = defaultdict(list)  # node -> dependents

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def node(
        self,
        depends_on: Optional[List[str]] = None,
        description: str = "",
    ) -> Callable:
        """Decorator to register a function as a graph node.

        Args:
            depends_on: Names of nodes that must complete before this one.
            description: Human-readable description of the node's purpose.

        Raises:
            ValueError: If a listed dependency name has not been registered
                yet. This catches typos in depends_on at definition time
                rather than silently producing a broken graph.
        """
        depends_on = depends_on or []

        def decorator(fn: Callable) -> Callable:
            node_name = fn.__name__

            # Validate that all declared dependencies are already registered.
            # Catches typos early instead of failing at execution time.
            # NOTE: This means nodes must be defined in dependency order.
            for dep in depends_on:
                if dep not in self._nodes:
                    raise ValueError(
                        f"Node '{node_name}' depends on '{dep}', which has not been "
                        f"registered yet. Define '{dep}' before '{node_name}'."
                    )

            graph_node = GraphNode(name=node_name, fn=fn, description=description)
            graph_node.upstream = list(depends_on)
            self._nodes[node_name] = graph_node

            # Wire up edges: each dependency gains this node as a dependent.
            for dep in depends_on:
                self._edges[dep].append(node_name)
                self._nodes[dep].downstream.append(node_name)

            return fn

        return decorator

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def get_node(self, name: str) -> GraphNode:
        """Return the GraphNode registered under *name*.

        Raises:
            KeyError: If no node with that name exists.
        """
        if name not in self._nodes:
            raise KeyError(f"No node named '{name}' in graph '{self.name}'.")
        return self._nodes[name]

    def node_names(self) -> List[str]:
        """Return a list of all registered node names (insertion order)."""
        return list(self._nodes.keys())

    # ------------------------------------------------------------------
    # Topological sort
    # ------------------------------------------------------------------

    def _topological_order(self) -> List[str]:
        """Return nodes in topological execution order using Kahn's algorithm.

        Raises:
            RuntimeError: If a cycle is detected in the graph.
        """
        in_degree: Dict[str, int] = {name: 0 for name in self._nodes}
        for node_name, dependents in self._edges.items():
            for dep in dependents:
                in_degree[dep] += 1

        # Start with nodes that have no dependencies.
        queue: deque[str] = deque(
            name for name, degree in in_degree.items() if degree == 0
        )
        order: List[str] = []

        while queue:
            # Sort candidates alphabetically for deterministic ordering within
            # the same dependency level -- makes test output predictable.
            current = queue.popleft()
            order.append(current)
            for dependent in sorted(self._edges[current]):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(order) != len(self._nodes):
            raise RuntimeError(
                f"Cycle detected in graph '{self.name}'. "
                "Execution order cannot be determined."
            )

        return order

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute all nodes in topological order.

        Args:
            context: Optional initial context dict passed to every node.
                     Nodes may read from and write to this shared dict.
                     Defaults to an empty dict if not provided.

        Returns:
            The final context dict after all nodes have run.
        """
        # Default to an empty context rather than using a mutable default arg.
        ctx: Dict[str, Any] = context if context is not None else {}

        order = self._topological_order()
        logger.info(
            "Executing graph '%s' | nodes: %s", self.name, " -> ".join(order)
        )

        for node_name in order:
            node = self._nodes[node_name]
            try:
                node.run(ctx)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Graph '%s': node '%s' raised an exception: %s",
                    self.name,
                    node_name,
                    exc,
                )
                raise

        return ctx
