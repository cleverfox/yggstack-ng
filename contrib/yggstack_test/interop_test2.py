#!/usr/bin/env python3
"""
Debug interop test for TCP forwarding/session stalls (Go <-> Rust).

This script is intentionally focused on TCP paths and repeated requests to
reproduce/debug intermittent remote-tcp issues.
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
RUST_ROOT = SCRIPT_DIR.parent.parent
RUST_BIN = RUST_ROOT / "target" / "debug" / "yggstack"
GO_DIR = Path(os.environ.get("YGGSTACK_GO_DIR", ""))
GO_BIN = GO_DIR / "yggstack" if GO_DIR else Path("")


def run(cmd, timeout=20, check=True):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(cmd)}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}"
        )
    return p


def start_proc(cmd, verbose=False):
    if verbose:
        kwargs = {
            "stdout": None,
            "stderr": None,
            "text": True,
            "preexec_fn": os.setsid,
        }
    else:
        # Redirect to /dev/null rather than subprocess.PIPE to avoid pipe
        # buffer deadlock: the child can produce more output than the 64 KB
        # pipe buffer holds, blocking its writes and freezing the process.
        devnull = open(os.devnull, "w")
        kwargs = {
            "stdout": devnull,
            "stderr": subprocess.STDOUT,
            "text": True,
            "preexec_fn": os.setsid,
        }
    p = subprocess.Popen(cmd, **kwargs)
    p._log_fh = devnull if not verbose else None
    return p


def stop_proc(p):
    if not p:
        return
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        p.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
    if hasattr(p, '_log_fh') and p._log_fh:
        p._log_fh.close()


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


def alloc_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class RawTCPServer:
    def __init__(self, host, port, payload):
        self.host = host
        self.port = port
        self.payload = payload
        self._stop = threading.Event()
        self._thread = None
        self._sock = None
        self._lock = threading.Lock()
        self.accepted = 0
        self.sent_bytes = 0

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self._sock.settimeout(0.5)

        def loop():
            while not self._stop.is_set():
                try:
                    c, _ = self._sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                try:
                    with self._lock:
                        self.accepted += 1
                    c.sendall(self.payload)
                    with self._lock:
                        self.sent_bytes += len(self.payload)
                finally:
                    c.close()

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._thread:
            self._thread.join(timeout=1)

    def stats(self):
        with self._lock:
            return {"accepted": self.accepted, "sent_bytes": self.sent_bytes}


def curl_verbose_contains(url, marker, proxy=None, timeout=6):
    cmd = ["curl", "-v", "--http0.9", "--max-time", str(timeout)]
    if proxy:
        cmd.extend(["-x", proxy])
    cmd.append(url)
    p = run(cmd, timeout=timeout + 3, check=False)
    combined = (p.stdout or "") + "\n" + (p.stderr or "")
    if marker not in combined:
        raise RuntimeError(
            f"marker {marker!r} not found, rc={p.returncode}\n"
            f"stdout:\n{p.stdout}\nstderr:\n{p.stderr}"
        )
    return combined


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--iterations", type=int, default=20)
    ap.add_argument("--sleep-ms", type=int, default=200)
    args = ap.parse_args()

    if not RUST_BIN.exists():
        print(f"[FAIL] Rust binary not found: {RUST_BIN}. Build with: cargo build -p yggstack")
        return 1
    if not GO_BIN.exists():
        print("[FAIL] Set YGGSTACK_GO_DIR to directory containing Go yggstack binary")
        return 1

    temp_dir = Path(tempfile.mkdtemp(prefix="yggstack_it2_"))
    print(f"[INFO] temp dir: {temp_dir}")

    go_http_port = alloc_port()
    rust_http_port = alloc_port()
    go_socks_port = alloc_port()
    rust_socks_port = alloc_port()
    go_local_tcp_port = alloc_port()
    rust_local_tcp_port = alloc_port()

    go_backend = RawTCPServer("127.0.0.1", go_http_port, b"NOTHTTP go-raw\r\n")
    rust_backend = RawTCPServer("127.0.0.1", rust_http_port, b"NOTHTTP rust-raw\r\n")

    go_proc = None
    rust_proc = None

    try:
        go_backend.start()
        rust_backend.start()

        go_cfg = json.loads(run([str(GO_BIN), "-genconf", "-json"]).stdout)
        go_cfg["AdminListen"] = "none"
        go_cfg["Listen"] = ["tcp://127.0.0.1:19401"]
        go_cfg["Peers"] = ["tcp://127.0.0.1:19402"]
        go_conf_path = temp_dir / "go.json"
        go_conf_path.write_text(json.dumps(go_cfg, indent=2))

        rust_cfg = json.loads(run([str(RUST_BIN), "--genconf"]).stdout)
        rust_cfg["AdminListen"] = "none"
        rust_cfg["Listen"] = ["tcp://127.0.0.1:19402"]
        rust_cfg["Peers"] = []
        rust_conf_path = temp_dir / "rust.json"
        rust_conf_path.write_text(json.dumps(rust_cfg, indent=2))

        go_addr = run([str(GO_BIN), "-useconffile", str(go_conf_path), "-address"]).stdout.strip()
        rust_addr = run([str(RUST_BIN), "--useconffile", str(rust_conf_path), "--address"]).stdout.strip()
        go_pk = run([str(GO_BIN), "-useconffile", str(go_conf_path), "-publickey"]).stdout.strip()
        rust_pk = run([str(RUST_BIN), "--useconffile", str(rust_conf_path), "--publickey"]).stdout.strip()
        print(f"[INFO] go={go_addr} rust={rust_addr}")

        go_cmd = [
            str(GO_BIN),
            "-useconffile",
            str(go_conf_path),
            "-socks",
            f"127.0.0.1:{go_socks_port}",
            "-remote-tcp",
            f"80:127.0.0.1:{go_http_port}",
            "-local-tcp",
            f"127.0.0.1:{go_local_tcp_port}:[{rust_addr}]:80",
        ]
        rust_cmd = [
            str(RUST_BIN),
            "--useconffile",
            str(rust_conf_path),
            "--socks",
            f"127.0.0.1:{rust_socks_port}",
            "--remote-tcp",
            f"80:127.0.0.1:{rust_http_port}",
            "--local-tcp",
            f"127.0.0.1:{rust_local_tcp_port}:[{go_addr}]:80",
            "--loglevel",
            "debug",
        ]

        go_proc = start_proc(go_cmd, verbose=args.verbose)
        rust_proc = start_proc(rust_cmd, verbose=args.verbose)

        if not wait_tcp("127.0.0.1", go_socks_port, timeout=8):
            raise RuntimeError(f"go socks did not start on {go_socks_port}")
        if not wait_tcp("127.0.0.1", rust_socks_port, timeout=8):
            raise RuntimeError(f"rust socks did not start on {rust_socks_port}")

        tests = [
            (
                "rust local -> go remote",
                lambda: curl_verbose_contains(
                    f"http://127.0.0.1:{rust_local_tcp_port}",
                    "NOTHTTP go-raw",
                    timeout=6,
                ),
            ),
            (
                "rust socks ip -> go",
                lambda: curl_verbose_contains(
                    f"http://[{go_addr}]:80",
                    "NOTHTTP go-raw",
                    proxy=f"socks5h://127.0.0.1:{rust_socks_port}",
                    timeout=6,
                ),
            ),
            (
                "rust socks pk -> go",
                lambda: curl_verbose_contains(
                    f"http://{go_pk}.pk.ygg:80",
                    "NOTHTTP go-raw",
                    proxy=f"socks5h://127.0.0.1:{rust_socks_port}",
                    timeout=6,
                ),
            ),
            (
                "go local -> rust remote",
                lambda: curl_verbose_contains(
                    f"http://127.0.0.1:{go_local_tcp_port}",
                    "NOTHTTP rust-raw",
                    timeout=6,
                ),
            ),
            (
                "go socks ip -> rust",
                lambda: curl_verbose_contains(
                    f"http://[{rust_addr}]:80",
                    "NOTHTTP rust-raw",
                    proxy=f"socks5h://127.0.0.1:{go_socks_port}",
                    timeout=6,
                ),
            ),
            (
                "go socks pk -> rust",
                lambda: curl_verbose_contains(
                    f"http://{rust_pk}.pk.ygg:80",
                    "NOTHTTP rust-raw",
                    proxy=f"socks5h://127.0.0.1:{go_socks_port}",
                    timeout=6,
                ),
            ),
        ]

        stats = {name: {"ok": 0, "fail": 0, "last_err": ""} for name, _ in tests}
        for i in range(1, args.iterations + 1):
            for name, fn in tests:
                try:
                    fn()
                    stats[name]["ok"] += 1
                except Exception as e:
                    stats[name]["fail"] += 1
                    stats[name]["last_err"] = str(e)
            time.sleep(args.sleep_ms / 1000.0)
            if i % 5 == 0 or i == args.iterations:
                print(f"[INFO] iteration {i}/{args.iterations}")

        print("\n[RESULT] per-path counters")
        hard_fail = False
        for name, _ in tests:
            s = stats[name]
            print(f" - {name}: ok={s['ok']} fail={s['fail']}")
            if s["fail"] > 0:
                hard_fail = True
                print(f"   last_err: {s['last_err']}")

        print(f"\n[RAW] go backend stats: {go_backend.stats()}")
        print(f"[RAW] rust backend stats: {rust_backend.stats()}")

        if hard_fail:
            print("[FAIL] interop_test2 detected forwarding instability")
            return 1

        print("[PASS] interop_test2 all repeated TCP checks succeeded")
        return 0
    finally:
        stop_proc(rust_proc)
        stop_proc(go_proc)
        go_backend.stop()
        rust_backend.stop()


if __name__ == "__main__":
    sys.exit(main())
