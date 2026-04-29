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

            for dep in depends_on:
                self._edges[dep].append(node_name)
                self._nodes[dep].downstream.append(node_name)

            return fn

        return decorator

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _topological_sort(self) -> List[str]:
        """Return nodes in topological order using Kahn's algorithm.

        Raises:
            RuntimeError: If a cycle is detected in the graph.
        """
        in_degree: Dict[str, int] = {name: 0 for name in self._nodes}
        for node_name in self._nodes:
            for dependent in self._edges[node_name]:
                in_degree[dependent] += 1

        # Start with all nodes that have no dependencies
        queue: deque[str] = deque(
            sorted(name for name, deg in in_degree.items() if deg == 0)
        )
        order: List[str] = []

        while queue:
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

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute all nodes in topological order.

        Args:
            context: Initial context dict passed to every node. A fresh
                empty dict is used if none is provided.

        Returns:
            The context dict after all nodes have run.
        """
        # I prefer starting with an empty dict rather than requiring callers
        # to always pass one in - makes quick ad-hoc runs less boilerplate.
        ctx: Dict[str, Any] = context if context is not None else {}
        order = self._topological_sort()
        logger.info("Executing graph '%s' | order: %s", self.name, order)

        for node_name in order:
            node = self._nodes[node_name]
            logger.info("[%s] Starting node: %s", self.name, node_name)
            node.run(ctx)
            logger.info("[%s] Finished node: %s", self.name, node_name)

        return ctx

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Graph(name={self.name!r}, nodes={list(self._nodes.keys())})"

    def summary(self) -> str:
        """Return a human-readable summary of the graph structure."""
        lines = [f"Graph: {self.name}"]
        for name, node in self._nodes.items():
            deps = ", ".join(node.upstream) or "(none)"
            desc = f" — {node.description}" if node.description else ""
            lines.append(f"  {name} (depends_on: {deps}){desc}")
        return "\n".join(lines)
