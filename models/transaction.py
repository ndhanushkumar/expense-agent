from pydantic import BaseModel
from typing import Optional,Literal

class Transaction(BaseModel):
    email_id: str
    amount: float
    type: str            
    merchant: Optional[str] = None
    upi_ref: Optional[str] = None
    date: str
    account: Optional[str] = None
    category: Literal["food", "entertainment", "utilities", "shopping", "other","persons"] = "other"