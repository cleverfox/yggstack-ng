# Yggstack Interop Smoke Test

Interop smoke test for:

- Go `yggstack` (reference implementation)
- Rust `yggstack` (this workspace, `crates/yggstack`)

The test validates that Rust `smoltcp` can communicate with a Go `yggstack`
node over Yggdrasil IPv6 without TUN/root privileges.

## What It Tests

1. Start a local HTTP service on `127.0.0.1:18080`
2. Start Go `yggstack` with:
   - a fixed TCP peering listener
   - `-remote-tcp 8080:127.0.0.1:18080`
3. Start Rust `yggstack` peered to Go with:
   - `--probe-tcp [<go-ygg-ipv6>]:8080`
4. Pass if Rust logs `Probe succeeded` and receives HTTP response bytes

## Prerequisites

1. Build Rust `yggstack`:

   ```bash
   cd /path/to/Yggdrasil-ng
   cargo build -p yggstack
   ```

2. Build Go `yggstack` and set `YGGSTACK_GO_DIR`:

   ```bash
   cd /path/to/yggstack
   go build -o yggstack ./cmd/yggstack
   export YGGSTACK_GO_DIR=/path/to/yggstack
   ```

3. Python 3.8+ (stdlib only).

## Run

```bash
cd /path/to/Yggdrasil-ng
python3 contrib/yggstack_test/interop_test.py
```

Full transport matrix (TCP + UDP, both directions, SOCKS and forwarding):

```bash
cd /path/to/Yggdrasil-ng
python3 contrib/yggstack_test/full_matrix_test.py
```

Single mode run (examples):

```bash
cd /path/to/Yggdrasil-ng
python3 contrib/yggstack_test/full_matrix_test.py --mode rust-rust
python3 contrib/yggstack_test/full_matrix_test.py --mode go-rust
python3 contrib/yggstack_test/full_matrix_test.py --mode go-go
```

### Options

```text
  -v, --verbose         show subprocess logs
  -t, --timeout SECS    probe timeout (default: 20)
  --tcp-only            run only TCP checks
  --mode MODE           one of: all, go-rust, rust-rust, go-go (default: all)
```
