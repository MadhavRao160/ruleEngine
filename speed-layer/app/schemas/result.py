from pydantic import BaseModel
from typing import Optional

class EvaluationResponse(BaseModel):
    """
    Guaranteed contract for engine decisions.
    """
    status: str
    liability: Optional[str] = "ISSUER"
    reason_code: Optional[str] = None
    message: Optional[str] = None