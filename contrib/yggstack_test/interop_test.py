#!/usr/bin/env python3
"""
Yggstack interop smoke test: Go yggstack <-> Rust yggstack (smoltcp probe).
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
RUST_ROOT = SCRIPT_DIR.parent.parent
RUST_BIN = RUST_ROOT / "target" / "debug" / "yggstack"
GO_DIR = Path(os.environ.get("YGGSTACK_GO_DIR", ""))
GO_BIN = GO_DIR / "yggstack" if GO_DIR else Path("")


def fail(msg):
    print(f"[FAIL] {msg}")
    sys.exit(1)


def run(cmd, timeout=20, check=True):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"{cmd} failed\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
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
        # buffer deadlock when the child produces more output than the
        # 64 KB pipe buffer holds.
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


def wait_port(host, port, timeout=10):
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
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("-t", "--timeout", type=int, default=20)
    args = ap.parse_args()

    if not RUST_BIN.exists():
        fail(f"Rust binary not found: {RUST_BIN}. Build with: cargo build -p yggstack")
    if not GO_BIN.exists():
        fail("Set YGGSTACK_GO_DIR to directory containing Go yggstack binary")

    temp_dir = Path(tempfile.mkdtemp(prefix="yggstack_it_"))
    http_proc = None
    go_proc = None
    rust_proc = None

    try:
        # 1) HTTP backend to expose via Go yggstack remote mapping.
        (temp_dir / "index.html").write_text("hello-from-yggstack-interop\n")
        http_proc = start_proc(["python3", "-m", "http.server", "18080", "--bind", "127.0.0.1"], verbose=args.verbose)
        if not wait_port("127.0.0.1", 18080, timeout=5):
            fail("HTTP backend did not start on 127.0.0.1:18080")

        # 2) Go yggstack config.
        conf_out = run([str(GO_BIN), "-genconf", "-json"]).stdout
        go_cfg = json.loads(conf_out)
        go_cfg["AdminListen"] = "none"
        go_cfg["Listen"] = ["tcp://127.0.0.1:19001"]
        go_cfg["Peers"] = ["tcp://127.0.0.1:19002"]
        go_cfg_path = temp_dir / "go-node.json"
        go_cfg_path.write_text(json.dumps(go_cfg, indent=2))

        # 3) Rust yggstack compat config.
        rust_cfg = {
            "PrivateKey": "",
            "Peers": ["tcp://127.0.0.1:19001"],
            "Listen": ["tcp://127.0.0.1:19002"],
            "AdminListen": "none",
            "AllowedPublicKeys": [],
            "NodeInfoPrivacy": False,
            "NodeInfo": {},
        }
        rust_cfg_path = temp_dir / "rust-node.json"
        rust_cfg_path.write_text(json.dumps(rust_cfg, indent=2))

        go_addr = run([str(GO_BIN), "-useconffile", str(go_cfg_path), "-address"]).stdout.strip()
        print(f"[INFO] Go Ygg address: {go_addr}")

        # 4) Start Go node with remote mapping.
        go_proc = start_proc([
            str(GO_BIN),
            "-useconffile",
            str(go_cfg_path),
            "-remote-tcp",
            "8080:127.0.0.1:18080"
            ],
            verbose=args.verbose,
        )
        time.sleep(2)

        # 5) Start Rust node with smoltcp probe.
        rust_cmd = [
            str(RUST_BIN),
            "--useconffile",
            str(rust_cfg_path),
            "-remote-tcp",
            "8080:127.0.0.1:18080",
            "--probe-tcp",
            f"[{go_addr}]:8080",
            "--loglevel",
            "info",
        ]
        rust_proc = start_proc(rust_cmd, verbose=args.verbose)

        # Wait until probe result appears.
        deadline = time.time() + args.timeout
        combined = ""
        success = False
        while time.time() < deadline:
            if rust_proc.poll() is not None:
                break
            if rust_proc.stdout is not None:
                line = rust_proc.stdout.readline()
                if line:
                    combined += line
                    if "Probe succeeded" in line:
                        success = True
                        break
            else:
                time.sleep(0.1)

        if not success:
            # Drain any final output.
            if rust_proc.stdout is not None:
                try:
                    combined += rust_proc.stdout.read() or ""
                except Exception:
                    pass
            print("[DEBUG] Rust output:")
            print(combined[-4000:])
            fail("Rust smoltcp probe did not report success")

        print("[PASS] Rust smoltcp successfully communicated with Go yggstack over Yggdrasil IPv6")
    finally:
        stop_proc(rust_proc)
        stop_proc(go_proc)
        stop_proc(http_proc)


if __name__ == "__main__":
    main()
