import argparse
import os
import sys
from .processor import process_file

def main():
    parser = argparse.ArgumentParser(description="Fold Transaction Transformer")
    parser.add_argument('--input', default='data', help='Input directory or file')
    parser.add_argument('--output', default='output', help='Output directory')
    parser.add_argument('--timezone', default='Asia/Kolkata', help='Target timezone for output dates (e.g. Asia/Kolkata)')
    parser.add_argument('--since', help='Extract transactions since this date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    input_path = args.input
    output_dir = args.output
    target_timezone = args.timezone
    since_date = args.since
    
    if os.path.isdir(input_path):
        print(f"Error: Input path '{input_path}' is a directory. Please provide a single CSV file.")
        sys.exit(1)
        
    if os.path.isfile(input_path):
        if not input_path.lower().endswith('.csv'):
            print(f"Error: Input file '{input_path}' is not a CSV file.")
            sys.exit(1)
        process_file(input_path, output_dir, target_timezone, since_date)
    else:
        print(f"Error: Input path not found: {input_path}")
        sys.exit(1)

if __name__ == "__main__":
    main()
