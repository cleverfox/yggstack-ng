#!/usr/bin/env python3
"""
Single remote-tcp forwarding example with two Rust yggstack nodes.

Flow:
1) Start local HTTP server on host.
2) Start node1 with remote mapping: Ygg TCP 80 -> local HTTP server.
3) Start node2 peered to node1 with local mapping: host TCP -> node1 Ygg TCP 80.
4) Verify with curl to node2 local forwarded port.
"""

import argparse
import http.server
import os
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BIN = SCRIPT_DIR.parent / "target" / "debug" / "yggstack"
NODE1_CONF = SCRIPT_DIR / "node1.json"
NODE2_CONF = SCRIPT_DIR / "node2.json"


class TinyHandler(http.server.BaseHTTPRequestHandler):
    body = b"hello-from-node1-http\n"

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, fmt, *args):
        return


def run(cmd, timeout=20, check=True):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(cmd)}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}"
        )
    return p


def start_proc(cmd, verbose=False):
    kwargs = {
        "stdout": None if verbose else subprocess.PIPE,
        "stderr": None if verbose else subprocess.STDOUT,
        "text": True,
        "preexec_fn": os.setsid,
    }
    return subprocess.Popen(cmd, **kwargs)


def stop_proc(p):
    if not p:
        return
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        p.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass


def wait_tcp(host, port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        try:
            s.connect((host, port))
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
        finally:
            s.close()
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--yggstack-bin", default=str(DEFAULT_BIN))
    ap.add_argument("--http-port", type=int, default=18080)
    ap.add_argument("--forward-port", type=int, default=18081)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    yggstack_bin = Path(args.yggstack_bin)
    if not yggstack_bin.exists():
        print(f"[FAIL] yggstack binary not found: {yggstack_bin}")
        print("Build with: cargo build -p yggstack")
        return 1
    if not NODE1_CONF.exists() or not NODE2_CONF.exists():
        print("[FAIL] expected node1.json and node2.json in compat/yggstack_example")
        return 1

    httpd = None
    http_thread = None
    node1 = None
    node2 = None

    try:
        socketserver.TCPServer.allow_reuse_address = True
        httpd = socketserver.TCPServer(("127.0.0.1", args.http_port), TinyHandler)
        http_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        http_thread.start()
        print(f"[INFO] HTTP server started on 127.0.0.1:{args.http_port}")

        node1_addr = run(
            [str(yggstack_bin), "--useconffile", str(NODE1_CONF), "--address"]
        ).stdout.strip()
        print(f"[INFO] node1 ygg address: {node1_addr}")

        node1_cmd = [
            str(yggstack_bin),
            "--useconffile",
            str(NODE1_CONF),
            "--remote-tcp",
            f"80:127.0.0.1:{args.http_port}",
            "--loglevel",
            "info",
        ]
        node2_cmd = [
            str(yggstack_bin),
            "--useconffile",
            str(NODE2_CONF),
            "--local-tcp",
            f"127.0.0.1:{args.forward_port}:[{node1_addr}]:80",
            "--loglevel",
            "info",
        ]

        node1 = start_proc(node1_cmd, verbose=args.verbose)
        node2 = start_proc(node2_cmd, verbose=args.verbose)

        if not wait_tcp("127.0.0.1", args.forward_port, timeout=12):
            raise RuntimeError(
                f"node2 local forward port did not open on 127.0.0.1:{args.forward_port}"
            )

        out = run(
            ["curl", "-sS", "--max-time", "10", f"http://127.0.0.1:{args.forward_port}"]
        ).stdout
        if out != TinyHandler.body.decode():
            raise RuntimeError(f"unexpected response body: {out!r}")

        print("[PASS] forwarding works")
        print(f"       curl http://127.0.0.1:{args.forward_port} -> node2 local-tcp")
        print(f"       node2 -> ygg [{node1_addr}]:80 -> node1 remote-tcp -> local HTTP")
        time.sleep(60)
        return 0
    except Exception as e:
        print(f"[FAIL] {e}")
        return 1
    finally:
        stop_proc(node2)
        stop_proc(node1)
        if httpd:
            httpd.shutdown()
            httpd.server_close()
        if http_thread:
            http_thread.join(timeout=1)


if __name__ == "__main__":
    sys.exit(main())
