import pandas as pd
import os
import pytz
from typing import List, Dict, Any, Iterable
from datetime import datetime
from .models import Transaction
from .interfaces import TransactionReader, TransactionWriter

class CsvReader(TransactionReader):
    def __init__(self, target_timezone: str = 'Asia/Kolkata'):
        self.target_timezone = pytz.timezone(target_timezone)

    def read(self, source: str) -> Iterable[Transaction]:
        print(f"Reading {source}...")
        dtype_map = {
            'account_number': str,
            'card_number': str,
            'card_name': str,
            'bank_name': str,
            'tracking_method': str,
            'type': str,
            'narration': str,
            'reference': str,
            'merchant': str,
            'category': str
        }
        
        try:
            df = pd.read_csv(source, dtype=dtype_map)
        except Exception as e:
            print(f"Error reading file: {e}")
            return []

        string_cols = [c for c in dtype_map.keys() if c in df.columns]
        df[string_cols] = df[string_cols].fillna('')

        transactions = []
        for _, row in df.iterrows():
            ts = self._parse_timestamp(row.get('txn_timestamp'))
            
            txn = Transaction(
                account_number=row.get('account_number', '').strip(),
                card_number=row.get('card_number', '').strip(),
                card_name=row.get('card_name', '').strip(),
                bank_name=row.get('bank_name', '').strip(),
                txn_timestamp=ts,
                amount=float(row.get('amount', 0.0)),
                current_balance=row.get('current_balance'),
                type=row.get('type', ''),
                narration=row.get('narration', ''),
                reference=row.get('reference', ''),
                merchant=row.get('merchant', ''),
                category=row.get('category', ''),
                tracking_method=row.get('tracking_method', '')
            )
            transactions.append(txn)
        return transactions

    def _parse_timestamp(self, ts_val):
        try:
            ts = pd.to_datetime(ts_val)
            if ts is not None and pd.notna(ts):
                if ts.tzinfo is None:
                    ts = pytz.utc.localize(ts)
                return ts.astimezone(self.target_timezone)
        except:
            pass
        return None

class CsvWriter(TransactionWriter):
    def write(self, grouped_data: Dict[str, List[Dict[str, Any]]], output_dir: str):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        print(f"Found {len(grouped_data)} unique accounts.")
        for key, rows in grouped_data.items():
            if not rows:
                continue
                
            out_df = pd.DataFrame(rows)
            cols = ['date', 'payee', 'notes', 'debit_amount', 'credit_amount']
            # Ensure columns exist, though rows should have them
            out_df = out_df[cols] if set(cols).issubset(out_df.columns) else out_df
            
            filename = f"{key}.csv"
            out_path = os.path.join(output_dir, filename)
            
            print(f"Writing {len(rows)} transactions to {out_path}")
            out_df.to_csv(out_path, index=False)
