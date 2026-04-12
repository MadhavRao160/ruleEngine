from pydantic import BaseModel
from typing import Optional


class EvaluationResponse(BaseModel):
    """
    Guaranteed contract for engine decisions.

    Example JSON:
    {
        "status": "APPROVED",
        "liability": "ISSUER",
        "reason_code": "WITHIN_MEAL_BUDGET",
        "message": "Transaction permitted."
    }
    """
    status: str
    liability: Optional[str] = "ISSUER"
    reason_code: Optional[str] = None
    message: Optional[str] = None