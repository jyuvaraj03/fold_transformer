from typing import Dict, Any
from .models import Transaction
from .interfaces import TransactionMapper

class CsvMapper(TransactionMapper):
    def map(self, txn: Transaction) -> Dict[str, Any]:
        amount = txn.amount
        type_val = (txn.type or "").upper()
        
        debit_amount = 0.0
        credit_amount = 0.0
        
        if type_val == 'DEBIT':
            debit_amount = abs(amount)
        elif type_val == 'CREDIT':
            credit_amount = abs(amount)
        else:
            if amount < 0:
                debit_amount = abs(amount)
            else:
                credit_amount = amount

        return {
            'date': txn.txn_timestamp.date().isoformat() if txn.txn_timestamp else '',
            'payee': txn.merchant or txn.narration,
            'notes': txn.narration if txn.merchant else txn.reference,
            'debit_amount': debit_amount,
            'credit_amount': credit_amount
        }
