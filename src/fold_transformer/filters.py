from typing import List, Iterable
from .models import Transaction
from .interfaces import TransactionFilter
from datetime import datetime

class DateFilter(TransactionFilter):
    def __init__(self, since_date: datetime = None):
        self.since_date = since_date

    def filter(self, transactions: Iterable[Transaction]) -> List[Transaction]:
        if not self.since_date:
            return list(transactions)
            
        filtered = []
        for txn in transactions:
            if txn.txn_timestamp:
                # Ensure timezone awareness compatibility is handled by the caller or model
                # But here we assume txn_timestamp is already localized/aware if since_date is.
                if txn.txn_timestamp >= self.since_date:
                    filtered.append(txn)
            else:
                # Keep transactions without timestamp? Or drop? 
                # Original logic dropped them if since_date was set.
                # "if ts and since_dt and ts < since_dt: continue"
                # This means if ts is None, it continues (is kept).
                # Wait, "if ts and since_dt and ts < since_dt" -> if ts is None, condition is False -> kept.
                filtered.append(txn)
        return filtered
