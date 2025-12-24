from typing import List, Dict
from .models import Transaction
from .interfaces import TransactionGrouper

class AccountGrouper(TransactionGrouper):
    def group(self, transactions: List[Transaction]) -> Dict[str, List[Transaction]]:
        grouped = {}
        for txn in transactions:
            key = txn.account_key
            safe_key = "".join([c for c in key if c.isalnum() or c in (' ', '_', '-')]).strip()
            
            if safe_key not in grouped:
                grouped[safe_key] = []
            grouped[safe_key].append(txn)
        return grouped
