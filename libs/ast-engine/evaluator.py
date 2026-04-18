import operator
from typing import Any, Dict, Callable, Optional


class EvaluationContext:
    """
    The memory bank for the AST Engine.
    Now upgraded to handle validated Pydantic objects instead of raw dicts.
    """

    def __init__(self, request_data: Any, state_data: Any):
        self.request_data = request_data
        self.state_data = state_data

    def resolve_variable(self, path: str) -> Any:
        parts = path.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid variable path: '{path}'. Expected 'source.field'")

        source, field = parts[0], parts[1]

        # Route to the correct data bucket
        target_object = self.request_data if source == "request" else self.state_data if source == "state" else None

        if target_object is None:
            raise ValueError(f"Unknown data source: '{source}'. Allowed: 'request', 'state'.")

        # Extract the field (Handles both Pydantic models and plain dictionaries)
        if hasattr(target_object, field):
            return getattr(target_object, field)
        elif isinstance(target_object, dict) and field in target_object:
            return target_object[field]
        else:
            raise KeyError(f"CRITICAL: Field '{field}' missing from '{source}'.")


class ASTEvaluator:
    """
    The core Engine. Navigates the JSON Policy and executes logic.
    """

    def __init__(self, policy_ast: Dict[str, Any], context: EvaluationContext):
        if "ast" not in policy_ast:
            raise ValueError("CRITICAL: Invalid policy format. Missing root 'ast'.")

        self.policy_ast = policy_ast
        self.context = context
        self._supported_operators = {
            "==": operator.eq, "!=": operator.ne,
            ">": operator.gt, ">=": operator.ge,
            "<": operator.lt, "<=": operator.le,
            "IN": lambda left, right: left in right if isinstance(right, (list, set, tuple)) else False,
            "NOT_IN": lambda left, right: left not in right if isinstance(right, (list, set, tuple)) else False
        }
        self._node_handlers: Dict[str, Callable[[Dict[str, Any]], Any]] = {
            "CONDITION_LEAF": self._eval_condition_leaf,
            "LOGICAL_NODE": self._eval_logical_node,
            "DECISION_BRANCH": self._eval_decision_branch
        }
        self._logical_handlers = {
            "AND": self._eval_and_strategy,
            "OR": self._eval_or_strategy
        }

    def evaluate(self) -> Any:
        return self._evaluate_node(self.policy_ast["ast"])

    def _evaluate_node(self, node: Dict[str, Any]) -> Any:
        node_type = node.get("type")
        specialist = self._node_handlers.get(node_type)
        if not specialist:
            raise ValueError(f"CRITICAL: Unknown node type '{node_type}'.")
        return specialist(node)

    def _eval_condition_leaf(self, node: Dict[str, Any]) -> bool:
        left_val = self.context.resolve_variable(node["field"])
        op = node["operator"]
        right_data = node["value"]

        if isinstance(right_data, str) and (right_data.startswith("state.") or right_data.startswith("request.")):
            right_val = self.context.resolve_variable(right_data)
        else:
            right_val = right_data

        return self._apply_operator(left_val, op, right_val)

    def _eval_logical_node(self, node: Dict[str, Any]) -> bool:
        op = str(node.get("operator", "")).upper()
        logic_function = self._logical_handlers.get(op)
        if not logic_function:
            raise ValueError(f"CRITICAL: Unsupported logical operator '{op}'")
        return logic_function(node.get("nodes", []))

    def _eval_and_strategy(self, children: list) -> bool:
        for child in children:
            if not self._evaluate_node(child): return False
        return True

    def _eval_or_strategy(self, children: list) -> bool:
        for child in children:
            if self._evaluate_node(child): return True
        return False

    def _eval_decision_branch(self, node: Dict[str, Any]) -> Any:
        for branch in node.get("branches", []):
            if self._evaluate_node(branch["condition"]):
                return branch["action"]
        return node.get("default_action")

    def _apply_operator(self, left: Any, op: str, right: Any) -> bool:
        op = str(op).upper()
        if op not in self._supported_operators:
            raise ValueError(f"CRITICAL: Unsupported operator '{op}'.")
        return self._supported_operators[op](left, right)