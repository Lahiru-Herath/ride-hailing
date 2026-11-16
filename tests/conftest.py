"""Make the repo root importable (observability/, storage/) when pytest is run from anywhere."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))