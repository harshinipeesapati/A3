# config.py
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Number of bucket processes
NUM_BUCKETS = 4

# Bucket ports: bucket 0 -> 9100, bucket 1 -> 9101, ...
BUCKET_BASE_PORT = 9100

DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

COORDINATOR_HOST = "127.0.0.1"
COORDINATOR_PORT = 9000
