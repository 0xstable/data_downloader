import json
from pathlib import Path

def generate_credential():
    file = 'config.json'
    filepath = Path(__file__).parent
    with open(filepath/file) as f:
        d = json.load(f)
    return d