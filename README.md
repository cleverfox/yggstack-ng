# yggstack-ng

Standalone Rust yggstack project.

- Contains `crates/yggstack` and interoperability tests in `contrib/yggstack_test`.
- Uses Yggdrasil-ng as a git submodule at `./yggdrasil`.

## Install (macOS, Homebrew)

Install from the tap (builds from source, including the `yggdrasil` submodule):

```bash
brew tap cleverfox/yggdrasil
brew trust cleverfox/yggdrasil    # Homebrew requires trusting any 3rd-party tap
brew install --HEAD yggstack-ng   # --HEAD until a release is tagged
```

This installs the `yggstack` command — a userspace Yggdrasil node that needs no
TUN device and no root, exposing a SOCKS proxy instead:

```bash
yggstack --autoconf --socks 127.0.0.1:1080
# then point a SOCKS5-aware client at 127.0.0.1:1080
```

### Peers in `--autoconf` mode

`--autoconf` generates an ephemeral config with no peers. To add peers without
writing a config file, set the `YGG_PEERS` environment variable to a
comma-separated list of peer URIs:

```bash
YGG_PEERS=tls://10.10.11.15:15123,quic://10.10.21.22:443 yggstack --autoconf
```

Supported schemes: `tcp`, `tls`, `ws`, `wss`, `quic`. `YGG_PEERS` also augments
(and de-duplicates against) peers from `--useconf`/`--useconffile`.

See `yggstack --help` for all options.

## Build

```bash
cargo build -p yggstack
```

## Tests

```bash
python3 contrib/yggstack_test/full_matrix_test.py
```
