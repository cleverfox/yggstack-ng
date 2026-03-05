# Yggstack Implementation Plan (Rust Workspace)

## Scope

- Supported now: Linux, macOS, FreeBSD
- Deferred: Windows (to be implemented by a separate contributor)

## Goals

- Keep `crates/yggdrasil` core changes minimal
- Add a separate `crates/yggstack` binary crate for user-space proxy/forwarding mode
- Preserve yggstack-compatible CLI/config where practical

## Phases

1. Workspace and crate scaffolding (`crates/yggstack`)
2. CLI and config compatibility layer (`-autoconf`, `-useconf`, `-useconffile`, `-genconf`, mapping flags)
3. `smoltcp` integration over `ReadWriteCloser`
4. SOCKS5 + `.pk.ygg` resolver + optional external nameserver
5. TCP/UDP local/remote mappings
6. Hardening and test expansion

## Testing

- Unit: mapping parser, resolver, config translation
- Integration: in-process multi-node path for SOCKS and forwarding
- Interop: rust yggstack with yggdrasil-go/yggstack
- Platform validation: Linux/macOS/FreeBSD CI and smoke tests
