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
            for dep in depends_on:
                if dep not in self._nodes:
                    raise ValueError(
                        f"Node '{node_name}' depends on '{dep}', "
                        f"which has not been registered yet."
                    )

            graph_node = GraphNode(name=node_name, fn=fn, description=description)
            graph_node.upstream = list(depends_on)

            for dep in depends_on:
                self._edges[dep].append(node_name)
                if dep in self._nodes:
                    self._nodes[dep].downstream.append(node_name)

            self._nodes[node_name] = graph_node
            logger.debug("Registered node '%s' with deps: %s", node_name, depends_on)
       
