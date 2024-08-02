"""
Main module for processing weather events from stdin and outputting results to stdout.

This module reads JSON-formatted weather events from standard input, processes them
using the weather module, and outputs the results as JSON to standard output.
"""

import json
import sys
from . import weather

def generate_input():
    """
    Generate input events from standard input.

    Yields:
        dict: A JSON-parsed event from each line of standard input.
    """
    for line in sys.stdin:
        yield json.loads(line)

for output in weather.process_events(generate_input()):
    print(json.dumps(output))
    