# yggstack-ng

Standalone Rust yggstack project.

- Contains `crates/yggstack` and interoperability tests in `contrib/yggstack_test`.
- Uses Yggdrasil-ng as a git submodule at `./yggdrasil`.

## Build

```bash
cargo build -p yggstack
```

## Tests

```bash
python3 contrib/yggstack_test/full_matrix_test.py
```
