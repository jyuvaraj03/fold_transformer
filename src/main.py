import argparse
import os
import sys
from .processor import process_file

def main():
    parser = argparse.ArgumentParser(description="Fold Transaction Transformer")
    parser.add_argument('--input', default='data', help='Input directory or file')
    parser.add_argument('--output', default='output', help='Output directory')
    
    args = parser.parse_args()
    
    input_path = args.input
    output_dir = args.output
    
    if os.path.isfile(input_path):
        process_file(input_path, output_dir)
    elif os.path.isdir(input_path):
        # Process all CSVs in directory
        for f in os.listdir(input_path):
            if f.lower().endswith('.csv'):
                full_path = os.path.join(input_path, f)
                process_file(full_path, output_dir)
    else:
        print(f"Input path not found: {input_path}")
        sys.exit(1)

if __name__ == "__main__":
    main()
