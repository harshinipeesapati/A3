# coordinator.py - FIXED WITH STABLE HASH
import socket
import threading
import os
from datetime import datetime

from config import (
    NUM_BUCKETS, BUCKET_BASE_PORT, COORDINATOR_HOST, COORDINATOR_PORT, LOG_DIR,
    DATA_DIR
)

LOG_FILE = os.path.join(LOG_DIR, "run.log")

def log(message: str):
    ts = datetime.utcnow().isoformat()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{ts} {message}\n")

def hash_key(key: str) -> int:
    """STABLE POLYNOMIAL HASH - distributes movieIds evenly across buckets"""
    h = 0
    for i, c in enumerate(key):
        h = (h * 31 + ord(c)) % 1000000007  # Large prime modulus
    return abs(h) % NUM_BUCKETS

def bucket_address(bucket_id: int):
    return "127.0.0.1", BUCKET_BASE_PORT + bucket_id

def send_to_bucket(bucket_id: int, payload: str) -> str:
    host, port = bucket_address(bucket_id)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2.0)
            s.connect((host, port))
            s.sendall((payload + "\n").encode("utf-8"))
            data = s.recv(4096).decode("utf-8")
            return data.strip()
    except:
        return "ERROR"

def range_from_bucket(bucket_id: int, k1: str, k2: str):
    resp = send_to_bucket(bucket_id, f"RANGE {k1} {k2}")
    lines = resp.splitlines()
    if not lines:
        return []
    try:
        n = int(lines[0])
    except ValueError:
        return []
    result = []
    for line in lines[1:1 + n]:
        if "\t" in line:
            key, value = line.split("\t", 1)
            result.append((key, value, bucket_id))
    return result

class CoordinatorServer:
    def __init__(self):
        self.host = COORDINATOR_HOST
        self.port = COORDINATOR_PORT
        log("COORDINATOR STARTED - Stable polynomial hash function active")

    def handle_client(self, conn: socket.socket, addr):
        with conn:
            data = conn.recv(4096).decode("utf-8")
            if not data:
                return
            parts = data.strip().split(" ", 2)
            cmd = parts[0].upper()

            if cmd == "INSERT" and len(parts) == 3:
                key, value = parts[1], parts[2]
                bucket_id = hash_key(key)
                bucket_file = os.path.join(DATA_DIR, f"bucket_{bucket_id}.txt")
                resp = send_to_bucket(bucket_id, f"PUT {key} {value}")
                log(f"INSERT key={key} bucket={bucket_id} file={bucket_file} value={value[:50]}...")
                conn.sendall((resp + "\n").encode("utf-8"))

            elif cmd == "SEARCH" and len(parts) >= 2:
                key = parts[1]
                bucket_id = hash_key(key)
                bucket_file = os.path.join(DATA_DIR, f"bucket_{bucket_id}.txt")
                resp = send_to_bucket(bucket_id, f"GET {key}")
                if "VALUE" in resp:
                    value = resp.split(" ", 1)[1] if " " in resp else ""
                    log(f"SEARCH key={key} bucket={bucket_id} file={bucket_file} value={value[:50]}...")
                else:
                    log(f"SEARCH key={key} bucket={bucket_id} file={bucket_file} resp={resp}")
                conn.sendall((resp + "\n").encode("utf-8"))

            elif cmd == "DELETE" and len(parts) >= 2:
                key = parts[1]
                bucket_id = hash_key(key)
                bucket_file = os.path.join(DATA_DIR, f"bucket_{bucket_id}.txt")
                resp = send_to_bucket(bucket_id, f"DEL {key}")
                log(f"DELETE key={key} bucket={bucket_id} file={bucket_file} resp={resp}")
                conn.sendall((resp + "\n").encode("utf-8"))

            elif cmd == "RANGE" and len(parts) >= 3:
                k1 = parts[1]
                k2 = parts[2]
                all_results = []
                for b in range(NUM_BUCKETS):
                    bucket_results = range_from_bucket(b, k1, k2)
                    all_results.extend(bucket_results)
                all_results.sort(key=lambda kv: kv[0])
                bucket_file_template = os.path.join(DATA_DIR, "bucket_{}.txt")
                for key, value, b in all_results:
                    log(f"RANGE key={key} bucket={b} file={bucket_file_template.format(b)} value={value[:50]}...")
                lines = [str(len(all_results))]
                for key, value, _b in all_results:
                    lines.append(f"{key}\t{value}")
                resp = "\n".join(lines)
                conn.sendall((resp + "\n").encode("utf-8"))

            else:
                conn.sendall(b"ERROR\n")

    def serve_forever(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Coordinator listening on {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                t = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                t.start()

if __name__ == "__main__":
    os.makedirs(LOG_DIR, exist_ok=True)
    open(LOG_FILE, "w").close()
    server = CoordinatorServer()
    server.serve_forever()
