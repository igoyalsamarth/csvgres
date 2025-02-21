import os
import sys
from pathlib import Path

# Get the absolute path to the project root directory
project_root = Path(__file__).parent.parent.absolute()
# Add both the project root and the csvgres directory to the Python path
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'transformer'))

def pytest_configure():
    """
    Called before pytest collects any tests.
    This ensures the csvgres module is in the path.
    """
    # You can add any additional test setup here if needed
    pass 