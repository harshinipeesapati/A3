# bucket_server.py
import socket
import threading
import os
from config import DATA_DIR, BUCKET_BASE_PORT

class BucketServer:
    def __init__(self, bucket_id: int):
        self.bucket_id = bucket_id
        self.host = "127.0.0.1"
        self.port = BUCKET_BASE_PORT + bucket_id
        self.data_file = os.path.join(DATA_DIR, f"bucket_{bucket_id}.txt")
        self.store = {}
        self.lock = threading.Lock()
        self._load_from_disk()

    def _load_from_disk(self):
        if not os.path.exists(self.data_file):
            return
        with open(self.data_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                if "\t" not in line:
                    continue
                key, value = line.split("\t", 1)
                self.store[key] = value

    def _persist_to_disk(self):
        # Simple rewrite strategy
        with open(self.data_file, "w", encoding="utf-8") as f:
            for k, v in self.store.items():
                f.write(f"{k}\t{v}\n")

    def handle_client(self, conn: socket.socket, addr):
        with conn:
            data = conn.recv(4096).decode("utf-8")
            if not data:
                return
            parts = data.strip().split(" ", 2)
            cmd = parts[0].upper()

            if cmd == "PUT" and len(parts) == 3:
                key, value = parts[1], parts[2]
                with self.lock:
                    self.store[key] = value
                    self._persist_to_disk()
                conn.sendall(b"OK\n")

            elif cmd == "GET" and len(parts) >= 2:
                key = parts[1]
                with self.lock:
                    if key in self.store:
                        value = self.store[key]
                        conn.sendall(f"VALUE {value}\n".encode("utf-8"))
                    else:
                        conn.sendall(b"NOT_FOUND\n")

            elif cmd == "DEL" and len(parts) >= 2:
                key = parts[1]
                with self.lock:
                    if key in self.store:
                        del self.store[key]
                        self._persist_to_disk()
                        conn.sendall(b"OK\n")
                    else:
                        conn.sendall(b"NOT_FOUND\n")

            elif cmd == "RANGE" and len(parts) >= 3:
                k1 = parts[1]
                k2 = parts[2]
                with self.lock:
                    keys_in_range = [
                        (k, self.store[k])
                        for k in self.store.keys()
                        if k1 <= k <= k2
                    ]
                keys_in_range.sort(key=lambda kv: kv[0])
                lines = [str(len(keys_in_range))]
                for k, v in keys_in_range:
                    lines.append(f"{k}\t{v}")
                resp = "\n".join(lines) + "\n"
                conn.sendall(resp.encode("utf-8"))

            else:
                conn.sendall(b"ERROR\n")

    def serve_forever(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(
                f"Bucket {self.bucket_id} listening on "
                f"{self.host}:{self.port}, file={self.data_file}"
            )
            while True:
                conn, addr = s.accept()
                t = threading.Thread(
                    target=self.handle_client, args=(conn, addr), daemon=True
                )
                t.start()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python bucket_server.py <bucket_id>")
        sys.exit(1)
    bucket_id = int(sys.argv[1])
    server = BucketServer(bucket_id)
    server.serve_forever()
