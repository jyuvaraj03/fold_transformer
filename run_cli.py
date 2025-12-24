import sys
import os

# Ensure src is in the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from fold_transformer.main import main

if __name__ == '__main__':
    main()
