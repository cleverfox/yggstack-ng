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

See `yggstack --help` for all options.

## Build

```bash
cargo build -p yggstack
```

## Tests

```bash
python3 contrib/yggstack_test/full_matrix_test.py
```
