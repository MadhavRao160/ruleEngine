import operator
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union, Callable

@dataclass
class EvaluationResult:
    """
    Standardized payload returning the final verdict from the AST Engine.
    """
    status: str
    liability: Optional[str] = None
    reason_code: Optional[str] = None
    message: Optional[str] = None


class EvaluationContext:
    """
    The memory bank for the AST Engine.
    Holds the real-time card swipe data and the hydrated history from DynamoDB.
    """

    def __init__(self, request_data: Dict[str, Any], state_data: Dict[str, Any]):
        self.request_data = request_data
        self.state_data = state_data

    def resolve_variable(self, path: str) -> Any:
        """
        Takes a string path (e.g., 'request.amount' or 'state.is_traveling')
        and extracts the actual numerical or boolean value from the dictionaries.
        """
        # Split the string at the dot (e.g., turns "request.amount" into ["request", "amount"])
        parts = path.split('.')

        if len(parts) != 2:
            raise ValueError(f"Invalid variable path format: '{path}'. Expected format: 'source.field'")

        source = parts[0]
        field = parts[1]

        # Route to the correct bucket based on the first word
        if source == "request":
            if field not in self.request_data:
                raise KeyError(f"CRITICAL: Field '{field}' is missing from the incoming request data.")
            return self.request_data[field]

        elif source == "state":
            if field not in self.state_data:
                raise KeyError(f"CRITICAL: Field '{field}' is missing from the DynamoDB state data.")
            return self.state_data[field]

        else:
            # Security firewall: Rejects anything that isn't 'request' or 'state'
            raise ValueError(f"Unknown data source: '{source}'. Allowed sources are 'request' or 'state'.")


class ASTEvaluator:
    """
    The core Engine. Navigates the JSON Policy (Map) and uses the
    EvaluationContext (Backpack) to resolve variables and execute logic.
    """

    def __init__(self, policy_ast: Dict[str, Any], context: 'EvaluationContext'):
        if "ast" not in policy_ast:
            raise ValueError("CRITICAL: Invalid policy format. Missing root 'ast' block.")

        self.policy_ast = policy_ast
        self.context = context
        self._supported_operators = {
            "==": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            ">=": operator.ge,
            "<": operator.lt,
            "<=": operator.le,
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
        """
        The Main Public Method. FastAPI calls this to start the engine.
        """
        # The engine always starts at the root 'ast' node
        root_node = self.policy_ast["ast"]

        # Hand the entire tree to the Traffic Cop
        final_result = self._evaluate_node(root_node)

        return final_result

    def _evaluate_node(self, node: Dict[str, Any]) -> Any:
        # 1. Look at the JSON map to find the string (e.g., "CONDITION_LEAF")
        node_type = node.get("type")

        # 2. Look up that string in our Contacts App
        specialist_function = self._node_handlers.get(node_type)

        # 3. Guardrail: What if the type isn't in our list?
        if not specialist_function:
            raise ValueError(f"CRITICAL: Unknown node type '{node_type}' intercepted.")

        # 4. Execute: Hand the JSON block to the correct specialist
        return specialist_function(node)

    def _eval_condition_leaf(self, node: Dict[str, Any]) -> bool:

        # STEP 1: Read the JSON map
        # Example node: {"field": "request.amount", "operator": "<=", "value": 50}
        left_string = node["field"]  # This becomes "request.amount"
        operator = node["operator"]  # This becomes "<="
        right_data = node["value"]  # This becomes 50

        # STEP 2: Translate the Left Side
        # Ask the backpack to turn the string "request.amount" into the real number 65
        real_left_number = self.context.resolve_variable(left_string)

        # STEP 3: Translate the Right Side
        # Is the right side a string that looks like a variable? (e.g., "state.budget")
        if type(right_data) is str and right_data.startswith("state."):
            real_right_number = self.context.resolve_variable(right_data)

        elif type(right_data) is str and right_data.startswith("request."):
            real_right_number = self.context.resolve_variable(right_data)

        # Otherwise, it's already a hardcoded number (like 50) or a list
        else:
            real_right_number = right_data

        # STEP 4: Hand the real numbers to the Math Sandbox
        # This translates to: self._apply_operator(65, "<=", 50)
        return self._apply_operator(real_left_number, operator, real_right_number)

    def _eval_logical_node(self, node: Dict[str, Any]) -> bool:
        """
        The Manager. Reads the JSON operator and routes the children
        to the correct strategy using the Registry (Contacts App).
        """
        # 1. Read the JSON map
        operator = str(node.get("operator", "")).upper()
        children = node.get("nodes", [])

        # 2. Look up the operator in our Contacts App (defined in __init__)
        logic_function = self._logical_handlers.get(operator)

        if not logic_function:
            raise ValueError(f"CRITICAL: Unsupported logical operator '{operator}'")

        # 3. Hand the children to the correct strategy tool
        return logic_function(children)


    def _eval_and_strategy(self, children: list) -> bool:
        """
        AND Strategy Tool: Loops through children and returns False
        the moment any child evaluates to False (Short-Circuit).
        """
        for child in children:
            if not self._evaluate_node(child):
                return False
        return True


    def _eval_or_strategy(self, children: list) -> bool:
        """
        OR Strategy Tool: Loops through children and returns True
        the moment any child evaluates to True (Short-Circuit).
        """
        for child in children:
            if self._evaluate_node(child):
                return True
        return False

    def _eval_decision_branch(self, node: Dict[str, Any]) -> Any:
        """
        The Strategist. Evaluates multiple rule branches top-to-bottom.
        Returns the action of the FIRST branch whose condition is True.
        """
        # 1. Read the JSON map
        branches = node.get("branches")
        default_action = node.get("default_action")

        # Guardrails
        if not isinstance(branches, list):
            raise ValueError("CRITICAL: DECISION_BRANCH 'branches' must be an array.")
        if not default_action:
            raise ValueError("CRITICAL: DECISION_BRANCH must have a 'default_action'.")

        # 2. Evaluate Branches (The Top-to-Bottom Strategy)
        for branch in branches:
            condition = branch.get("condition")
            action = branch.get("action")

            if not condition or not action:
                 raise ValueError("CRITICAL: Each branch must have a 'condition' and 'action'.")

            # Ask the Traffic Cop to evaluate the IF condition
            is_match = self._evaluate_node(condition)

            # 3. The Winner Takes All
            if is_match:
                # We found our exact scenario. Return the THEN action (e.g., APPROVE)
                return action

        # 4. The Fallback
        # If we checked every single rule and none were true, return the default.
        return default_action

    def _apply_operator(self, left: Any, op: str, right: Any) -> bool:
        """
        The Secure Sandbox. Safely executes math by looking up the
        operator string in the engine's registry.
        """
        op = str(op).upper()
        if op not in self._supported_operators:
            raise ValueError(f"CRITICAL: Unsupported or malicious operator '{op}' intercepted.")
        math_function = self._supported_operators[op]
        return math_function(left, right)