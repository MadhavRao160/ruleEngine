from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TransactionRequest(BaseModel):
    """
    Validates incoming card swipe data.

    Example JSON:
    {
        "transaction_id": "tx-12345",
        "user_id": "madhav_01",
        "amount": 45.50,
        "currency": "USD",
        "merchant_name": "UberEats",
        "mcc": "5812",
        "category": "food"
    }
    """
    transaction_id: str
    user_id: str
    amount: float = Field(..., gt=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    merchant_name: str
    mcc: str
    category: str
    pos_entry_mode: Optional[str] = "01"
    location: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)