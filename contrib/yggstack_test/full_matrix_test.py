#!/usr/bin/env python3
"""
Full yggstack transport matrix test.

Default mode: run all combinations (`go-rust`, `rust-rust`, `go-go`).
Use `--mode` to run a specific combination.

Covers:
- TCP via local/remote forwarding
- TCP via SOCKS5 (IP and .pk.ygg)
- UDP echo via local/remote forwarding
- UDP echo via SOCKS5 UDP ASSOCIATE
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
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
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


def wait_tcp(host, port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.4)
        try:
            s.connect((host, port))
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
        finally:
            s.close()
    return False


def alloc_port(family=socket.AF_INET, socktype=socket.SOCK_STREAM):
    s = socket.socket(family, socktype)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class TinyHTTPServer:
    def __init__(self, host, port, body):
        self.host = host
        self.port = port
        self.body = body.encode()
        self._stop = threading.Event()
        self._thread = None
        self._sock = None

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(50)
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
                    _ = c.recv(4096)
                    resp = (
                        b"HTTP/1.1 200 OK\r\n"
                        + f"Content-Length: {len(self.body)}\r\n".encode()
                        + b"Connection: close\r\n\r\n"
                        + self.body
                    )
                    c.sendall(resp)
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


class UDPEchoServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._stop = threading.Event()
        self._thread = None
        self._sock = None

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((self.host, self.port))
        self._sock.settimeout(0.5)

        def loop():
            while not self._stop.is_set():
                try:
                    data, addr = self._sock.recvfrom(8192)
                except socket.timeout:
                    continue
                except OSError:
                    break
                try:
                    self._sock.sendto(data, addr)
                except OSError:
                    break

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
        self._sock.listen(50)
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


def curl_fetch(url, proxy=None, timeout=10):
    cmd = ["curl", "-sS", "--max-time", str(timeout)]
    if proxy:
        cmd.extend(["-x", proxy])
    cmd.append(url)
    return run(cmd, timeout=timeout + 3).stdout


def curl_verbose_contains(url, marker, proxy=None, timeout=10):
    cmd = ["curl", "-v", "--max-time", str(timeout)]
    if proxy:
        cmd.extend(["-x", proxy])
    cmd.append(url)
    p = run(cmd, timeout=timeout + 3, check=False)
    if p.returncode == 28:
        raise RuntimeError(
            f"command failed: {' '.join(cmd)}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}"
        )
    combined = (p.stdout or "") + "\n" + (p.stderr or "")
    if marker not in combined:
        raise RuntimeError(
            f"marker {marker!r} not found in curl -v output\n"
            f"returncode={p.returncode}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}"
        )
    return combined


def udp_echo_direct(host, port, payload, timeout=3):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(payload, (host, port))
        data, _ = s.recvfrom(8192)
        return data
    finally:
        s.close()


def retry_until(name, fn, timeout=20, interval=0.5):
    deadline = time.time() + timeout
    last_err = ""
    while time.time() < deadline:
        try:
            fn()
            print(f"[READY] {name}")
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(interval)
    raise RuntimeError(f"{name} not ready in {timeout}s: {last_err}")


def _encode_socks_addr(host):
    # IPv6 literal?
    try:
        ip6 = socket.inet_pton(socket.AF_INET6, host)
        return b"\x04" + ip6
    except OSError:
        pass
    # IPv4 literal?
    try:
        ip4 = socket.inet_pton(socket.AF_INET, host)
        return b"\x01" + ip4
    except OSError:
        pass
    b = host.encode()
    if len(b) > 255:
        raise ValueError("domain too long")
    return b"\x03" + bytes([len(b)]) + b


def socks_udp_echo(proxy_host, proxy_port, target_host, target_port, payload, timeout=4):
    # 1) TCP control channel.
    ctrl = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    ctrl.settimeout(timeout)
    try:
        # greeting
        ctrl.sendall(b"\x05\x01\x00")
        if ctrl.recv(2) != b"\x05\x00":
            raise RuntimeError("SOCKS auth negotiation failed")

        # UDP ASSOCIATE request with 0.0.0.0:0
        ctrl.sendall(b"\x05\x03\x00\x01\x00\x00\x00\x00\x00\x00")
        rep = ctrl.recv(4)
        if len(rep) != 4 or rep[1] != 0x00:
            raise RuntimeError("SOCKS UDP ASSOCIATE rejected")
        atyp = rep[3]
        if atyp == 0x01:
            relay_addr = socket.inet_ntop(socket.AF_INET, ctrl.recv(4))
        elif atyp == 0x04:
            relay_addr = socket.inet_ntop(socket.AF_INET6, ctrl.recv(16))
        elif atyp == 0x03:
            ln = ctrl.recv(1)[0]
            relay_addr = ctrl.recv(ln).decode()
        else:
            raise RuntimeError("invalid SOCKS BND atyp")
        relay_port = int.from_bytes(ctrl.recv(2), "big")
        # Some SOCKS servers reply with an unspecified bind address
        # (0.0.0.0 / ::). In that case, send UDP datagrams to the SOCKS
        # control endpoint host we already connected to.
        if relay_addr in ("0.0.0.0", "::"):
            relay_addr = proxy_host

        # 2) UDP relay packet.
        relay_family = socket.AF_INET6 if ":" in relay_addr else socket.AF_INET
        u = socket.socket(relay_family, socket.SOCK_DGRAM)
        u.settimeout(timeout)
        try:
            hdr = b"\x00\x00\x00" + _encode_socks_addr(target_host) + target_port.to_bytes(2, "big")
            u.sendto(hdr + payload, (relay_addr, relay_port))
            data, _ = u.recvfrom(8192)
        finally:
            u.close()

        # Parse response header.
        if len(data) < 4 or data[2] != 0:
            raise RuntimeError("invalid SOCKS UDP response")
        atyp = data[3]
        off = 4
        if atyp == 0x01:
            off += 4
        elif atyp == 0x04:
            off += 16
        elif atyp == 0x03:
            ln = data[off]
            off += 1 + ln
        else:
            raise RuntimeError("invalid SOCKS UDP response atyp")
        off += 2  # dst port
        return data[off:]
    finally:
        ctrl.close()


def parse_modes(mode_opt):
    if mode_opt == "all":
        return ["go-rust", "rust-rust", "go-go"]
    return [mode_opt]


def impl_pair_for_mode(mode):
    if mode == "go-rust":
        return ("go", "rust")
    if mode == "rust-rust":
        return ("rust", "rust")
    if mode == "go-go":
        return ("go", "go")
    raise ValueError(f"unsupported mode: {mode}")


def genconf_cmd(impl):
    if impl == "go":
        return [str(GO_BIN), "-genconf", "-json"]
    return [str(RUST_BIN), "--genconf"]


def addr_cmd(impl, conf_path):
    if impl == "go":
        return [str(GO_BIN), "-useconffile", str(conf_path), "-address"]
    return [str(RUST_BIN), "--useconffile", str(conf_path), "--address"]


def pk_cmd(impl, conf_path):
    if impl == "go":
        return [str(GO_BIN), "-useconffile", str(conf_path), "-publickey"]
    return [str(RUST_BIN), "--useconffile", str(conf_path), "--publickey"]


def node_cmd(
    impl,
    conf_path,
    socks_port,
    remote_http_port,
    local_tcp_port,
    peer_addr,
    loglevel,
    tcp_only,
    remote_udp_port,
    local_udp_port,
):
    if impl == "go":
        cmd = [
            str(GO_BIN),
            "-useconffile",
            str(conf_path),
            "-loglevel",
            loglevel,
            "-socks",
            f"127.0.0.1:{socks_port}",
            "-remote-tcp",
            f"80:127.0.0.1:{remote_http_port}",
            "-local-tcp",
            f"127.0.0.1:{local_tcp_port}:[{peer_addr}]:80",
        ]
        if not tcp_only:
            cmd.extend(
                [
                    "-remote-udp",
                    f"1111:127.0.0.1:{remote_udp_port}",
                    "-local-udp",
                    f"127.0.0.1:{local_udp_port}:[{peer_addr}]:1111",
                ]
            )
        return cmd

    cmd = [
        str(RUST_BIN),
        "--useconffile",
        str(conf_path),
        "--socks",
        f"127.0.0.1:{socks_port}",
        "--remote-tcp",
        f"80:127.0.0.1:{remote_http_port}",
        "--local-tcp",
        f"127.0.0.1:{local_tcp_port}:[{peer_addr}]:80",
        "--loglevel",
        loglevel,
    ]
    if not tcp_only:
        cmd.extend(
            [
                "--remote-udp",
                f"1111:127.0.0.1:{remote_udp_port}",
                "--local-udp",
                f"127.0.0.1:{local_udp_port}:[{peer_addr}]:1111",
            ]
        )
    return cmd


def run_matrix_mode(mode, args):
    impl_a, impl_b = impl_pair_for_mode(mode)
    label_a = f"node-a({impl_a})"
    label_b = f"node-b({impl_b})"

    temp_dir = Path(tempfile.mkdtemp(prefix=f"yggstack_matrix_{mode.replace('-', '_')}_"))
    print(f"[INFO] temp dir: {temp_dir}")

    # Host-local service and forwarding ports (dynamic to avoid collisions).
    a_http_port = alloc_port(socktype=socket.SOCK_STREAM)
    b_http_port = alloc_port(socktype=socket.SOCK_STREAM)
    a_udp_port = alloc_port(socktype=socket.SOCK_DGRAM)
    b_udp_port = alloc_port(socktype=socket.SOCK_DGRAM)
    a_socks_port = alloc_port(socktype=socket.SOCK_STREAM)
    b_socks_port = alloc_port(socktype=socket.SOCK_STREAM)
    a_local_tcp_port = alloc_port(socktype=socket.SOCK_STREAM)
    b_local_tcp_port = alloc_port(socktype=socket.SOCK_STREAM)
    a_local_udp_port = alloc_port(socktype=socket.SOCK_DGRAM)
    b_local_udp_port = alloc_port(socktype=socket.SOCK_DGRAM)

    # Local backends.
    if args.raw_tcp_backend:
        a_http = RawTCPServer("127.0.0.1", a_http_port, b"NOTHTTP node-a-raw\r\n")
        b_http = RawTCPServer("127.0.0.1", b_http_port, b"NOTHTTP node-b-raw\r\n")
    else:
        a_http = TinyHTTPServer("127.0.0.1", a_http_port, "node-a-http")
        b_http = TinyHTTPServer("127.0.0.1", b_http_port, "node-b-http")
    a_udp = UDPEchoServer("127.0.0.1", a_udp_port)
    b_udp = UDPEchoServer("127.0.0.1", b_udp_port)

    a_proc = None
    b_proc = None
    results = []

    try:
        a_http.start()
        b_http.start()
        a_udp.start()
        b_udp.start()

        # Configs.
        a_cfg = json.loads(run(genconf_cmd(impl_a)).stdout)
        a_cfg["AdminListen"] = "none"
        a_cfg["Listen"] = ["tcp://127.0.0.1:19201"]
        a_cfg["Peers"] = ["tcp://127.0.0.1:19202"]
        a_conf_path = temp_dir / "a.json"
        a_conf_path.write_text(json.dumps(a_cfg, indent=2))

        b_cfg = json.loads(run(genconf_cmd(impl_b)).stdout)
        # Keep a single deterministic link (A -> B) to avoid dual-link churn.
        b_cfg["Peers"] = []
        b_cfg["Listen"] = ["tcp://127.0.0.1:19202"]
        b_cfg["AdminListen"] = "none"
        b_conf_path = temp_dir / "b.json"
        b_conf_path.write_text(json.dumps(b_cfg, indent=2))

        a_addr = run(addr_cmd(impl_a, a_conf_path)).stdout.strip()
        a_pk = run(pk_cmd(impl_a, a_conf_path)).stdout.strip()
        b_addr = run(addr_cmd(impl_b, b_conf_path)).stdout.strip()
        b_pk = run(pk_cmd(impl_b, b_conf_path)).stdout.strip()
        print(f"[INFO] mode={mode} {label_a}={a_addr} {label_b}={b_addr}")

        # Start nodes.
        a_cmd = node_cmd(
            impl=impl_a,
            conf_path=a_conf_path,
            socks_port=a_socks_port,
            remote_http_port=a_http_port,
            local_tcp_port=a_local_tcp_port,
            peer_addr=b_addr,
            loglevel="debug",
            tcp_only=args.tcp_only,
            remote_udp_port=a_udp_port,
            local_udp_port=a_local_udp_port,
        )
        b_cmd = node_cmd(
            impl=impl_b,
            conf_path=b_conf_path,
            socks_port=b_socks_port,
            remote_http_port=b_http_port,
            local_tcp_port=b_local_tcp_port,
            peer_addr=a_addr,
            loglevel="debug",
            tcp_only=args.tcp_only,
            remote_udp_port=b_udp_port,
            local_udp_port=b_local_udp_port,
        )

        a_proc = start_proc(a_cmd, verbose=args.verbose)
        b_proc = start_proc(b_cmd, verbose=args.verbose)

        if not wait_tcp("127.0.0.1", a_socks_port, timeout=8):
            raise RuntimeError(f"{label_a} socks did not start on {a_socks_port}")
        if not wait_tcp("127.0.0.1", b_socks_port, timeout=8):
            raise RuntimeError(f"{label_b} socks did not start on {b_socks_port}")

        retry_until(
            f"{label_b}->{label_a} tcp forwarding",
            lambda: (
                curl_verbose_contains(
                    f"http://127.0.0.1:{b_local_tcp_port}",
                    "NOTHTTP node-a-raw",
                    timeout=4,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-a-http" or (_ for _ in ()).throw(RuntimeError(f"got {out!r}")))(
                    curl_fetch(f"http://127.0.0.1:{b_local_tcp_port}", timeout=4).strip()
                )
            ),
            timeout=20,
        )
        retry_until(
            f"{label_a}->{label_b} tcp forwarding",
            lambda: (
                curl_verbose_contains(
                    f"http://127.0.0.1:{a_local_tcp_port}",
                    "NOTHTTP node-b-raw",
                    timeout=4,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-b-http" or (_ for _ in ()).throw(RuntimeError(f"got {out!r}")))(
                    curl_fetch(f"http://127.0.0.1:{a_local_tcp_port}", timeout=4).strip()
                )
            ),
            timeout=20,
        )
        if not args.tcp_only:
            retry_until(
                f"{label_b}->{label_a} udp forwarding",
                lambda: (
                    (lambda out: out == b"warm-ba" or (_ for _ in ()).throw(RuntimeError(f"got {out!r}")))(
                        udp_echo_direct("127.0.0.1", b_local_udp_port, b"warm-ba", timeout=2)
                    )
                ),
                timeout=20,
            )
            if args.strict_udp_readiness:
                retry_until(
                    f"{label_a}->{label_b} udp forwarding",
                    lambda: (
                        (lambda out: out == b"warm-ab" or (_ for _ in ()).throw(RuntimeError(f"got {out!r}")))(
                            udp_echo_direct("127.0.0.1", a_local_udp_port, b"warm-ab", timeout=2)
                        )
                    ),
                    timeout=20,
                )
            else:
                try:
                    retry_until(
                        f"{label_a}->{label_b} udp forwarding",
                        lambda: (
                            (lambda out: out == b"warm-ab" or (_ for _ in ()).throw(RuntimeError(f"got {out!r}")))(
                                udp_echo_direct("127.0.0.1", a_local_udp_port, b"warm-ab", timeout=2)
                            )
                        ),
                        timeout=20,
                    )
                except Exception as e:
                    print(f"[WARN] {label_a}->{label_b} udp forwarding not ready (continuing): {e}")

        def check(name, fn, expect_pass=True, retries=1, retry_delay=1.0):
            ok = False
            err = ""
            for attempt in range(1, retries + 1):
                try:
                    fn()
                    ok = True
                    err = ""
                    break
                except Exception as e:
                    ok = False
                    err = str(e)
                    if attempt < retries:
                        time.sleep(retry_delay)

            status = "PASS" if ok else ("XFAIL" if not expect_pass else "FAIL")
            print(f"[{status}] {name}" + (f" :: {err}" if err else ""))
            results.append((name, ok, expect_pass, err))

        # TCP tests via local/remote forwarding and SOCKS.
        check(
            f"tcp {label_b} local-tcp -> {label_a} remote-tcp",
            (
                lambda: curl_verbose_contains(
                    f"http://127.0.0.1:{b_local_tcp_port}",
                    "NOTHTTP node-a-raw",
                    timeout=8,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-a-http" or (_ for _ in ()).throw(RuntimeError(f"unexpected body: {out!r}")))(
                    curl_fetch(f"http://127.0.0.1:{b_local_tcp_port}", timeout=8).strip()
                )
            ),
            expect_pass=True,
            retries=3,
            retry_delay=1.0,
        )
        check(
            f"tcp {label_b} socks ip -> {label_a}",
            (
                lambda: curl_verbose_contains(
                    f"http://[{a_addr}]:80",
                    "NOTHTTP node-a-raw",
                    proxy=f"socks5h://127.0.0.1:{b_socks_port}",
                    timeout=8,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-a-http" or (_ for _ in ()).throw(RuntimeError(f"unexpected body: {out!r}")))(
                    curl_fetch(f"http://[{a_addr}]:80", proxy=f"socks5h://127.0.0.1:{b_socks_port}", timeout=8).strip()
                )
            ),
            expect_pass=True,
            retries=3,
            retry_delay=1.0,
        )
        check(
            f"tcp {label_b} socks .pk.ygg -> {label_a}",
            (
                lambda: curl_verbose_contains(
                    f"http://{a_pk}.pk.ygg:80",
                    "NOTHTTP node-a-raw",
                    proxy=f"socks5h://127.0.0.1:{b_socks_port}",
                    timeout=8,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-a-http" or (_ for _ in ()).throw(RuntimeError(f"unexpected body: {out!r}")))(
                    curl_fetch(f"http://{a_pk}.pk.ygg:80", proxy=f"socks5h://127.0.0.1:{b_socks_port}", timeout=8).strip()
                )
            ),
            expect_pass=True,
            retries=3,
            retry_delay=1.0,
        )

        check(
            f"tcp {label_a} local-tcp -> {label_b} remote-tcp",
            (
                lambda: curl_verbose_contains(
                    f"http://127.0.0.1:{a_local_tcp_port}",
                    "NOTHTTP node-b-raw",
                    timeout=8,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-b-http" or (_ for _ in ()).throw(RuntimeError(f"unexpected body: {out!r}")))(
                    curl_fetch(f"http://127.0.0.1:{a_local_tcp_port}", timeout=8).strip()
                )
            ),
            expect_pass=True,
            retries=5,
            retry_delay=2.0,
        )
        check(
            f"tcp {label_a} socks ip -> {label_b}",
            (
                lambda: curl_verbose_contains(
                    f"http://[{b_addr}]:80",
                    "NOTHTTP node-b-raw",
                    proxy=f"socks5h://127.0.0.1:{a_socks_port}",
                    timeout=8,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-b-http" or (_ for _ in ()).throw(RuntimeError(f"unexpected body: {out!r}")))(
                    curl_fetch(f"http://[{b_addr}]:80", proxy=f"socks5h://127.0.0.1:{a_socks_port}", timeout=8).strip()
                )
            ),
            expect_pass=True,
            retries=5,
            retry_delay=2.0,
        )
        check(
            f"tcp {label_a} socks .pk.ygg -> {label_b}",
            (
                lambda: curl_verbose_contains(
                    f"http://{b_pk}.pk.ygg:80",
                    "NOTHTTP node-b-raw",
                    proxy=f"socks5h://127.0.0.1:{a_socks_port}",
                    timeout=8,
                )
                if args.raw_tcp_backend
                else (lambda out: out == "node-b-http" or (_ for _ in ()).throw(RuntimeError(f"unexpected body: {out!r}")))(
                    curl_fetch(f"http://{b_pk}.pk.ygg:80", proxy=f"socks5h://127.0.0.1:{a_socks_port}", timeout=8).strip()
                )
            ),
            expect_pass=True,
            retries=5,
            retry_delay=2.0,
        )

        if not args.tcp_only:
            # UDP tests.
            check(
                f"udp {label_b} local-udp -> {label_a} remote-udp",
                lambda: (lambda out: out == b"ping-ba" or (_ for _ in ()).throw(RuntimeError(f"unexpected udp echo: {out!r}")))(
                    udp_echo_direct("127.0.0.1", b_local_udp_port, b"ping-ba")
                ),
                expect_pass=True,
            )
            check(
                f"udp {label_b} socks -> {label_a}",
                lambda: (lambda out: out == b"ping-sba" or (_ for _ in ()).throw(RuntimeError(f"unexpected udp socks echo: {out!r}")))(
                    socks_udp_echo("127.0.0.1", b_socks_port, a_addr, 1111, b"ping-sba")
                ),
                expect_pass=True,
            )
            check(
                f"udp {label_a} local-udp -> {label_b} remote-udp",
                lambda: (lambda out: out == b"ping-ab" or (_ for _ in ()).throw(RuntimeError(f"unexpected udp echo: {out!r}")))(
                    udp_echo_direct("127.0.0.1", a_local_udp_port, b"ping-ab")
                ),
                expect_pass=True,
            )
            check(
                f"udp {label_a} socks -> {label_b}",
                lambda: (lambda out: out == b"ping-sab" or (_ for _ in ()).throw(RuntimeError(f"unexpected udp socks echo: {out!r}")))(
                    socks_udp_echo("127.0.0.1", a_socks_port, b_addr, 1111, b"ping-sab")
                ),
                expect_pass=True,
            )

    finally:
        stop_proc(a_proc)
        stop_proc(b_proc)
        a_http.stop()
        b_http.stop()
        if args.raw_tcp_backend:
            print(f"[RAW] {label_a} backend stats: {a_http.stats()}")
            print(f"[RAW] {label_b} backend stats: {b_http.stats()}")
        a_udp.stop()
        b_udp.stop()

    hard_fail = [r for r in results if (r[2] and not r[1])]
    xfail_fail = [r for r in results if ((not r[2]) and (not r[1]))]
    if hard_fail:
        print(f"\n[SUMMARY:{mode}] hard failures:")
        for name, _, _, err in hard_fail:
            print(f" - {name}: {err}")
        return False

    if xfail_fail and not args.allow_xfail:
        print(f"\n[SUMMARY:{mode}] expected-fail cases currently failing:")
        for name, _, _, err in xfail_fail:
            print(f" - {name}: {err}")
        print("Use --allow-xfail to treat these as non-fatal.")
        return False

    print(f"\n[SUMMARY:{mode}] matrix complete.")
    if xfail_fail:
        print(f"[INFO] expected-fail count: {len(xfail_fail)}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument(
        "--mode",
        choices=["all", "go-rust", "rust-rust", "go-go"],
        default="all",
        help="test mode (default: all)",
    )
    ap.add_argument("--allow-xfail", action="store_true", help="do not fail run on known Rust-unimplemented cases")
    ap.add_argument("--tcp-only", action="store_true", help="run only TCP forwarding/SOCKS tests")
    ap.add_argument(
        "--raw-tcp-backend",
        action="store_true",
        help="use raw TCP responders (non-HTTP) for tcp checks to isolate proxy transport",
    )
    ap.add_argument(
        "--strict-udp-readiness",
        action="store_true",
        help="fail early if UDP forwarding readiness checks do not pass",
    )
    ap.add_argument("-t", "--timeout", type=int, default=20)
    args = ap.parse_args()

    if not RUST_BIN.exists():
        print(f"[FAIL] Rust binary not found: {RUST_BIN}. Build with: cargo build -p yggstack")
        sys.exit(1)
    modes = parse_modes(args.mode)
    if any(("go" in m) for m in modes) and not GO_BIN.exists():
        print("[FAIL] Set YGGSTACK_GO_DIR to directory containing Go yggstack binary")
        sys.exit(1)

    failures = []
    for mode in modes:
        print(f"\n[MODE] {mode}")
        ok = run_matrix_mode(mode, args)
        if not ok:
            failures.append(mode)

    if failures:
        print("\n[FINAL] failing modes:")
        for m in failures:
            print(f" - {m}")
        sys.exit(1)

    print("\n[FINAL] all requested modes passed.")


if __name__ == "__main__":
    main()
