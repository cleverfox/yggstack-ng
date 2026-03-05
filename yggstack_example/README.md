# yggstack Single Remote-TCP Example

This example tests one option only:

- `node1`: `--remote-tcp 80:127.0.0.1:18080`
- `node2`: `--local-tcp 127.0.0.1:18081:[node1_ygg_addr]:80`

`curl http://127.0.0.1:18081` should return data from the local HTTP server behind `node1`.

## Files

- `node1.json`: compat config for first Rust node (listens on `tcp://127.0.0.1:19301`)
- `node2.json`: compat config for second Rust node (peers to `node1`)
- `run_single_remote_tcp.py`: end-to-end runner

## Run

From repository root:

```bash
cargo build -p yggstack
python3 compat/yggstack_example/run_single_remote_tcp.py
```

Optional:

```bash
python3 compat/yggstack_example/run_single_remote_tcp.py --verbose
python3 compat/yggstack_example/run_single_remote_tcp.py --http-port 19080 --forward-port 19081
```
