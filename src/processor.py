import pandas as pd
import os
from .models import Transaction
from datetime import datetime
import pytz

def process_file(input_path: str, output_dir: str, target_timezone: str = 'Asia/Kolkata'):
    """
    Reads the CSV file, processes transactions, and writes to account-specific CSVs.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    print(f"Reading {input_path}...")
    try:
        # Define types for identifier columns to prevent float conversion (e.g. "1234" -> 1234.0)
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
        df = pd.read_csv(input_path, dtype=dtype_map)
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Fill NaNs for string columns with empty string
    string_cols = [c for c in dtype_map.keys() if c in df.columns]
    df[string_cols] = df[string_cols].fillna('')

    # Group by account logic
    grouped = {}
    
    tz = pytz.timezone(target_timezone)
    
    for idx, row in df.iterrows():
        try:
            ts = pd.to_datetime(row.get('txn_timestamp'))
            if ts is not None and ts.tzinfo is None:
                # Assume UTC as per instructions
                ts = pytz.utc.localize(ts)
            if ts is not None:
                ts = ts.astimezone(tz)
        except:
            ts = None

        txn = Transaction(
            account_number=row.get('account_number', '').strip(),
            card_number=row.get('card_number', '').strip(),
            card_name=row.get('card_name', '').strip(),
            bank_name=row.get('bank_name', '').strip(),
            txn_timestamp=ts,
            amount=row.get('amount', 0.0),
            current_balance=row.get('current_balance'),
            type=row.get('type', ''),
            narration=row.get('narration', ''),
            reference=row.get('reference', ''),
            merchant=row.get('merchant', ''),
            category=row.get('category', ''),
            tracking_method=row.get('tracking_method', '')
        )
        
        key = txn.account_key
        # Sanitize key for filename
        safe_key = "".join([c for c in key if c.isalnum() or c in (' ', '_', '-')]).strip()
        
        if safe_key not in grouped:
            grouped[safe_key] = []
        
        # Prepare processed row
        amount = float(row.get('amount', 0.0))
        type_val = row.get('type', '').upper()
        
        debit_amount = 0.0
        credit_amount = 0.0
        
        if type_val == 'DEBIT':
            debit_amount = abs(amount)
        elif type_val == 'CREDIT':
            credit_amount = abs(amount)
        else:
            # Fallback to sign if type is unknown
            if amount < 0:
                debit_amount = abs(amount)
            else:
                credit_amount = amount

        processed_row = {
            'date': ts.date().isoformat() if ts else '',
            'payee': txn.merchant or txn.narration,
            'notes': txn.narration if txn.merchant else txn.reference,
            'debit_amount': debit_amount,
            'credit_amount': credit_amount
        }
        
        grouped[safe_key].append(processed_row)
        
    # Write outputs
    print(f"Found {len(grouped)} unique accounts.")
    for key, rows in grouped.items():
        out_df = pd.DataFrame(rows)
        # Reorder columns as requested
        cols = ['date', 'payee', 'notes', 'debit_amount', 'credit_amount']
        out_df = out_df[cols]
        
        filename = f"{key}.csv"
        out_path = os.path.join(output_dir, filename)
        
        print(f"Writing {len(rows)} transactions to {out_path}")
        out_df.to_csv(out_path, index=False)
