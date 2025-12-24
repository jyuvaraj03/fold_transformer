from dataclasses import dataclass
from typing import Optional
import datetime

@dataclass
class Transaction:
    account_number: str
    card_number: str
    card_name: str
    bank_name: str
    txn_timestamp: datetime.datetime
    amount: float
    current_balance: Optional[float]
    type: str
    narration: str
    reference: str
    merchant: str
    category: str
    tracking_method: str
    
    # Original raw line or dict could be stored if needed
    
    @property
    def account_key(self) -> str:
        """
        Generates a unique key for grouping transactions.
        Priority:
        1. Bank Name + Account Number
        2. Card Name + Card Number
        3. Tracking Method + (Bank Name OR Card Name)
        """
        
        # Clean up values (strip whitespace)
        bank = (self.bank_name or "").strip()
        acc_num = (self.account_number or "").strip()
        card_name = (self.card_name or "").strip()
        card_num = (self.card_number or "").strip()
        
        if bank and acc_num:
            return f"{bank}_{acc_num}"
        
        if card_name and card_num:
            return f"{card_name}_{card_num}"
            
        # Fallback
        identifier = bank or card_name or "Unknown"
        return f"{self.tracking_method}_{identifier}"
