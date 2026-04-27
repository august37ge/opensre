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
        """
        depends_on = depends_on or []

        def decorator(fn: Callable) -> Callable:
            node_name = fn.__name__
            graph_node = GraphNode(name=node_name, fn=fn, description=description)
            graph_node.upstream = list(depends_on)

            for dep in depends_on:
                self._edges[dep].append(node_name)
                if dep in self._nodes:
                    self._nodes[dep].downstream.append(node_name)

            self._nodes[node_name] = graph_node
            logger.debug("Registered node '%s' with deps: %s", node_name, depends_on)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _topological_order(self) -> List[str]:
        """Return node names in topological execution order (Kahn's algorithm)."""
        in_degree: Dict[str, int] = {name: 0 for name in self._nodes}
        for node in self._nodes.values():
            for dep in node.upstream:
                if dep not in in_degree:
                    raise ValueError(
                        f"Node '{node.name}' depends on unknown node '{dep}'"
                    )
                in_degree[node.name] += 1

        queue: deque[str] = deque(
            name for name, deg in in_degree.items() if deg == 0
        )
        order: List[str] = []
        visited: Set[str] = set()

        while queue:
            current = queue.popleft()
            order.append(current)
            visited.add(current)
            for dependent in self._edges.get(current, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(order) != len(self._nodes):
            raise RuntimeError(
                "Cycle detected in graph '%s'; execution aborted." % self.name
            )
        return order

    def execute(self, initial_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute all nodes in dependency order.

        Args:
            initial_context: Seed data passed into the shared context dict.

        Returns:
            The final context dict after all nodes have run.
        """
        context: Dict[str, Any] = initial_context or {}
        order = self._topological_order()
        logger.info("Executing graph '%s' — %d nodes", self.name, len(order))

        for node_name in order:
            node = self._nodes[node_name]
            try:
                result = node.run(context)
                if result is not None:
                    context[node_name] = result
            except Exception as exc:  # noqa: BLE001
                logger.error("Node '%s' failed: %s", node_name, exc)
                raise

        logger.info("Graph '%s' completed successfully.", self.name)
        return context
