"""
Entry point for BCN CLI

Allows running as: python -m bcn <command> [args]
"""

import sys
from bcn.cli import main

if __name__ == "__main__":
    sys.exit(main())
