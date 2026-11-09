#!/usr/bin/env python3
"""Check local write/query/flush/restart using an existing MynaDB binary.

Uses only Python's standard library, loopback listeners and temporary data.
The optional transcript is evidence from this run, not a benchmark.
"""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def unused_ports():
    sockets = [socket.socket(), socket.socket()]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def run(binary, work, record):
    http, flight = unused_ports()
    config = work / "quickstart.toml"
    config.write_text(f'''[server]
http_host = "127.0.0.1"
http_port = {http}
flight_host = "127.0.0.1"
flight_port = {flight}
mysql_enabled = false
[storage]
backend = "local"
data_dir = "{work / 'data'}"
[schema]
strategy = "service_as_schema"
tag_keys = ["service"]
default_name = "default"
prefix_numeric = true
[cache]
flush_interval_secs = 3600
max_size_mb = 256
[wal]
enabled = false
[query_cache]
enabled = false
[retention]
enabled = false
[logging]
level = "info"
log_dir = "{work / 'logs'}"
file_prefix = "smoke"
''')
    base = f"http://127.0.0.1:{http}"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(path, body=None):
        data = None if body is None else body.encode()
        req = urllib.request.Request(base + path, data=data)
        if data is not None:
            req.add_header("Content-Type", "text/plain")
        with opener.open(req, timeout=30) as response:
            text = response.read().decode()
            status = response.status
        if path != "/health":
            record.append({"path": path, "request": body, "status": status, "response": text})
        return text

    def ready(process):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"Server exited with {process.returncode}")
            try:
                request("/health")
                record.append({"step": "health", "status": "passed"})
                return
            except (OSError, urllib.error.URLError):
                time.sleep(0.25)
        raise TimeoutError("Server did not become healthy within 60 seconds")

    def stop(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError("Graceful shutdown timed out")

    def query_and_check():
        rows = json.loads(request("/query/quickstart", "SELECT host, usage FROM default.cpu ORDER BY host"))
        expected = [{"host": "server01", "usage": 23.5}, {"host": "server02", "usage": 45.0}]
        if rows != expected:
            raise AssertionError(f"Unexpected query rows: {rows!r}")

    command = [str(binary), "-c", str(config), "standalone"]
    record.append({"command": command, "wal_enabled": False, "automatic_flush_seconds": 3600})
    with (work / "server.log").open("w") as log:
        process = subprocess.Popen(command, cwd=work, stdout=log, stderr=subprocess.STDOUT)
        try:
            ready(process)
            stamp = time.time_ns()
            request("/write/quickstart", f"cpu,host=server01 usage=23.5 {stamp}\ncpu,host=server02 usage=45.0 {stamp+1}\n")
            query_and_check()
            record.append({"step": "query-before-flush", "status": "passed"})
            request("/flush", "")
            files = list((work / "data").rglob("*.parquet"))
            if not files:
                raise AssertionError("Flush produced no Parquet files")
            record.append({"step": "flush", "parquet_files": len(files), "status": "passed"})
            stop(process)
            process = subprocess.Popen(command, cwd=work, stdout=log, stderr=subprocess.STDOUT)
            ready(process)
            query_and_check()
            record.append({"step": "query-after-restart", "status": "passed"})
        finally:
            stop(process)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--transcript", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f"Binary not found: {binary}")
    record = []
    with tempfile.TemporaryDirectory(prefix="mynadb-quickstart-") as directory:
        work = Path(directory)
        try:
            run(binary, work, record)
        except Exception:
            record.append({"status": "failed", "server_log": (work / "server.log").read_text() if (work / "server.log").exists() else ""})
            raise
        finally:
            if args.transcript:
                args.transcript.write_text(json.dumps(record, indent=2) + "\n")
    print("PASS: health, write, query, explicit flush, restart, persisted query, cleanup")


if __name__ == "__main__":
    main()
