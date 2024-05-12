import gzip
import json
import os
import random
import string

# Path to the output gzip file
output_path = "../random_json.gz"

# Function to generate random JSON objects as strings
def generate_random_json():
    return json.dumps({
        "id": ''.join(random.choices(string.ascii_letters + string.digits, k=10)),
        "value": random.randint(1, 100),
        "timestamp": random.randint(1577836800, 1704062400)
    })

# Create a 1GB gzip file with newline-separated JSON entries
size_limit = 1 * 1024 * 1024 * 1024  # 1GB

with gzip.open(output_path, 'wt') as f:
    size = 0
    while size < size_limit:
        json_entry = generate_random_json() + '\n'
        f.write(json_entry)
        size += len(json_entry.encode('utf-8'))

