use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;

use yggdrasil::address::addr_for_key;

use crate::mapping::MappingSpec;
use crate::smolstack::SmolStack;
use crate::socks::SocksListen;

pub async fn run_local_tcp_mapping(stack: Arc<SmolStack>, mapping: MappingSpec) -> Result<(), String> {
    let listen_host = mapping
        .first
        .host
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let listen_addr = format!("{listen_host}:{}", mapping.first.port);
    let listener = TcpListener::bind(&listen_addr)
        .await
        .map_err(|e| format!("bind local tcp mapping {listen_addr}: {e}"))?;

    let remote_host = mapping
        .second
        .host
        .ok_or_else(|| "missing remote mapping host".to_string())?;
    let remote_ip = remote_host
        .parse::<Ipv6Addr>()
        .map_err(|e| format!("invalid remote ygg IPv6 address {remote_host}: {e}"))?;
    let remote_port = mapping.second.port;

    tracing::info!(
        "Mapping local TCP {} -> Yggdrasil [{}]:{}",
        listen_addr,
        remote_ip,
        remote_port
    );

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .map_err(|e| format!("accept local tcp mapping failed: {e}"))?;
        let st = stack.clone();
        tokio::spawn(async move {
            if let Err(e) = st.proxy_tcp_tokio(stream, remote_ip, remote_port).await {
                tracing::debug!("local tcp mapping session {peer} failed: {e}");
            }
        });
    }
}

pub async fn run_socks_server(
    stack: Arc<SmolStack>,
    listen: SocksListen,
    nameserver: Option<String>,
) -> Result<(), String> {
    match listen {
        SocksListen::Tcp(addr) => {
            let listener = TcpListener::bind(&addr)
                .await
                .map_err(|e| format!("bind socks listener {addr}: {e}"))?;
            tracing::info!("SOCKS5 listening on {addr}");
            loop {
                let (stream, peer) = listener
                    .accept()
                    .await
                    .map_err(|e| format!("accept socks connection failed: {e}"))?;
                let st = stack.clone();
                let ns = nameserver.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_socks_client(stream, st, ns).await {
                        tracing::debug!("socks session {peer} failed: {e}");
                    }
                });
            }
        }
        #[cfg(unix)]
        SocksListen::Unix(path) => {
            use tokio::net::UnixListener;
            let _ = std::fs::remove_file(&path);
            let listener = UnixListener::bind(&path)
                .map_err(|e| format!("bind socks unix listener {path}: {e}"))?;
            tracing::info!("SOCKS5 listening on unix socket {path}");
            loop {
                let (stream, _) = listener
                    .accept()
                    .await
                    .map_err(|e| format!("accept unix socks connection failed: {e}"))?;
                let st = stack.clone();
                let ns = nameserver.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_socks_client(stream, st, ns).await {
                        tracing::debug!("unix socks session failed: {e}");
                    }
                });
            }
        }
    }
}

async fn handle_socks_client<S>(
    mut stream: S,
    stack: Arc<SmolStack>,
    nameserver: Option<String>,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Greeting: VER, NMETHODS, METHODS...
    let mut head = [0u8; 2];
    stream
        .read_exact(&mut head)
        .await
        .map_err(|e| format!("socks read greeting: {e}"))?;
    if head[0] != 0x05 {
        return Err("only SOCKS5 is supported".to_string());
    }
    let nmethods = head[1] as usize;
    let mut methods = vec![0u8; nmethods];
    stream
        .read_exact(&mut methods)
        .await
        .map_err(|e| format!("socks read methods: {e}"))?;
    if !methods.contains(&0x00) {
        stream
            .write_all(&[0x05, 0xff])
            .await
            .map_err(|e| format!("socks write auth reject: {e}"))?;
        return Err("no-auth SOCKS method (0x00) is required".to_string());
    }
    stream
        .write_all(&[0x05, 0x00])
        .await
        .map_err(|e| format!("socks write auth select: {e}"))?;

    // Request: VER CMD RSV ATYP DST.ADDR DST.PORT
    let mut req = [0u8; 4];
    stream
        .read_exact(&mut req)
        .await
        .map_err(|e| format!("socks read request: {e}"))?;
    if req[0] != 0x05 {
        return Err("invalid socks request version".to_string());
    }
    let cmd = req[1];

    let target_host = match req[3] {
        0x01 => {
            let mut ipv4 = [0u8; 4];
            stream
                .read_exact(&mut ipv4)
                .await
                .map_err(|e| format!("socks read ipv4 addr: {e}"))?;
            std::net::Ipv4Addr::from(ipv4).to_string()
        }
        0x03 => {
            let mut len = [0u8; 1];
            stream
                .read_exact(&mut len)
                .await
                .map_err(|e| format!("socks read domain len: {e}"))?;
            let mut domain = vec![0u8; len[0] as usize];
            stream
                .read_exact(&mut domain)
                .await
                .map_err(|e| format!("socks read domain: {e}"))?;
            String::from_utf8(domain).map_err(|e| format!("invalid socks domain utf8: {e}"))?
        }
        0x04 => {
            let mut ipv6 = [0u8; 16];
            stream
                .read_exact(&mut ipv6)
                .await
                .map_err(|e| format!("socks read ipv6 addr: {e}"))?;
            std::net::Ipv6Addr::from(ipv6).to_string()
        }
        _ => return Err("unsupported socks address type".to_string()),
    };

    let mut port_buf = [0u8; 2];
    stream
        .read_exact(&mut port_buf)
        .await
        .map_err(|e| format!("socks read port: {e}"))?;
    let target_port = u16::from_be_bytes(port_buf);

    match cmd {
        0x01 => {
            let target_ip = resolve_target_host(&target_host, nameserver.as_deref()).await?;
            // Success reply: BND=:: port 0.
            stream
                .write_all(&[
                    0x05, 0x00, 0x00, 0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0,
                ])
                .await
                .map_err(|e| format!("socks write success response: {e}"))?;
            stack.proxy_tcp_tokio(stream, target_ip, target_port).await
        }
        0x03 => handle_socks_udp_associate(stream, stack, nameserver.as_deref()).await,
        _ => {
            stream
                .write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await
                .map_err(|e| format!("socks write command reject: {e}"))?;
            Err("only CONNECT and UDP ASSOCIATE are supported".to_string())
        }
    }
}

async fn handle_socks_udp_associate<S>(
    mut stream: S,
    stack: Arc<SmolStack>,
    nameserver: Option<&str>,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let relay = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind socks udp relay: {e}"))?;
    let relay_addr = relay
        .local_addr()
        .map_err(|e| format!("udp relay local_addr: {e}"))?;
    let port = relay_addr.port().to_be_bytes();

    // Reply with relay endpoint.
    stream
        .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, port[0], port[1]])
        .await
        .map_err(|e| format!("socks write udp-associate response: {e}"))?;

    // Handle UDP datagrams until the TCP control stream closes.
    let relay = Arc::new(relay);
    let mut tcp_buf = [0u8; 1];
    let mut buf = vec![0u8; 65535];
    loop {
        tokio::select! {
            // TCP stream close signals end of UDP association per RFC 1928.
            tcp_res = stream.read(&mut tcp_buf) => {
                match tcp_res {
                    Ok(0) | Err(_) => return Ok(()),
                    Ok(_) => {} // ignore any data on TCP stream
                }
            }
            udp_res = relay.recv_from(&mut buf) => {
                let (n, client_addr) = udp_res
                    .map_err(|e| format!("socks udp relay recv: {e}"))?;
                if n < 10 {
                    tracing::debug!("socks udp packet too short ({n} bytes), skipping");
                    continue;
                }
                if buf[2] != 0 {
                    tracing::debug!("socks udp fragmentation not supported, skipping");
                    continue;
                }
                let mut off = 3;
                let target_host = match buf[off] {
                    0x01 => {
                        off += 1;
                        if n < off + 4 {
                            tracing::debug!("socks udp malformed ipv4 header");
                            continue;
                        }
                        let mut ip = [0u8; 4];
                        ip.copy_from_slice(&buf[off..off + 4]);
                        off += 4;
                        std::net::Ipv4Addr::from(ip).to_string()
                    }
                    0x03 => {
                        off += 1;
                        if n < off + 1 {
                            tracing::debug!("socks udp malformed domain len");
                            continue;
                        }
                        let ln = buf[off] as usize;
                        off += 1;
                        if n < off + ln {
                            tracing::debug!("socks udp malformed domain");
                            continue;
                        }
                        let d = match String::from_utf8(buf[off..off + ln].to_vec()) {
                            Ok(d) => d,
                            Err(e) => {
                                tracing::debug!("socks udp domain utf8: {e}");
                                continue;
                            }
                        };
                        off += ln;
                        d
                    }
                    0x04 => {
                        off += 1;
                        if n < off + 16 {
                            tracing::debug!("socks udp malformed ipv6 header");
                            continue;
                        }
                        let mut ip = [0u8; 16];
                        ip.copy_from_slice(&buf[off..off + 16]);
                        off += 16;
                        std::net::Ipv6Addr::from(ip).to_string()
                    }
                    _ => {
                        tracing::debug!("unsupported socks udp atyp {}", buf[off]);
                        continue;
                    }
                };
                if n < off + 2 {
                    tracing::debug!("socks udp malformed port");
                    continue;
                }
                let target_port = u16::from_be_bytes([buf[off], buf[off + 1]]);
                off += 2;
                let payload = buf[off..n].to_vec();

                let st = stack.clone();
                let relay2 = relay.clone();
                let ns = nameserver.map(|s| s.to_string());
                tokio::spawn(async move {
                    let target_ip = match resolve_target_host(&target_host, ns.as_deref()).await {
                        Ok(ip) => ip,
                        Err(e) => {
                            tracing::debug!("socks udp resolve {target_host} failed: {e}");
                            return;
                        }
                    };
                    match st.udp_roundtrip(target_ip, target_port, &payload, Duration::from_secs(4)).await {
                        Ok(resp) => {
                            let mut out = Vec::with_capacity(3 + 1 + 16 + 2 + resp.len());
                            out.extend_from_slice(&[0, 0, 0, 0x04]);
                            out.extend_from_slice(&target_ip.octets());
                            out.extend_from_slice(&target_port.to_be_bytes());
                            out.extend_from_slice(&resp);
                            if let Err(e) = relay2.send_to(&out, client_addr).await {
                                tracing::debug!("socks udp relay send to {client_addr} failed: {e}");
                            }
                        }
                        Err(e) => {
                            tracing::debug!("socks udp roundtrip to [{}]:{} failed: {e}", target_ip, target_port);
                        }
                    }
                });
            }
        }
    }
}

async fn resolve_target_host(host: &str, nameserver: Option<&str>) -> Result<Ipv6Addr, String> {
    if let Ok(ip) = host.parse::<Ipv6Addr>() {
        return Ok(ip);
    }

    if host.ends_with(".pk.ygg") {
        let mut name = host.trim_end_matches(".pk.ygg");
        if let Some(last) = name.rsplit('.').next() {
            name = last;
        }
        let pk = hex::decode(name).map_err(|e| format!("invalid .pk.ygg name '{host}': {e}"))?;
        if pk.len() != 32 {
            return Err(format!("invalid .pk.ygg key length for '{host}'"));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&pk);
        let addr = addr_for_key(&key);
        return Ok(std::net::Ipv6Addr::from(addr.0));
    }

    if let Some(ns) = nameserver {
        tracing::warn!(
            "nameserver '{}' configured, but DNS-over-Ygg is not implemented yet; using system resolver",
            ns
        );
    }

    let mut addrs = tokio::net::lookup_host((host, 0))
        .await
        .map_err(|e| format!("resolve host '{host}': {e}"))?;
    if let Some(v6) = addrs.find_map(|a| {
        if let std::net::SocketAddr::V6(v6) = a {
            Some(*v6.ip())
        } else {
            None
        }
    }) {
        return Ok(v6);
    }

    Err(format!("host '{host}' did not resolve to an IPv6 address"))
}

pub async fn run_remote_tcp_mapping(
    stack: Arc<SmolStack>,
    mapping: MappingSpec,
) -> Result<(), String> {
    let listen_port = mapping.first.port;
    let local_host = mapping
        .second
        .host
        .unwrap_or_else(|| "::1".to_string());
    let local_addr = format!("{local_host}:{}", mapping.second.port);
    tracing::info!(
        "Mapping Yggdrasil TCP port {} -> local TCP {}",
        listen_port, local_addr
    );
    stack
        .clone()
        .run_remote_tcp_listener_forward(listen_port, &local_addr)
        .await
}

pub async fn run_local_udp_mapping(stack: Arc<SmolStack>, mapping: MappingSpec) -> Result<(), String> {
    let listen_host = mapping
        .first
        .host
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let listen_addr = format!("{listen_host}:{}", mapping.first.port);
    let sock = UdpSocket::bind(&listen_addr)
        .await
        .map_err(|e| format!("bind local udp mapping {listen_addr}: {e}"))?;
    let remote_host = mapping
        .second
        .host
        .ok_or_else(|| "missing local-udp remote host".to_string())?;
    let remote_ip = remote_host
        .parse::<Ipv6Addr>()
        .map_err(|e| format!("invalid local-udp remote IPv6 {remote_host}: {e}"))?;
    let remote_port = mapping.second.port;

    tracing::info!(
        "Mapping local UDP {} -> Yggdrasil [{}]:{}",
        listen_addr,
        remote_ip,
        remote_port
    );

    let sock = Arc::new(sock);
    let mut buf = vec![0u8; 65535];
    loop {
        let (n, from) = sock
            .recv_from(&mut buf)
            .await
            .map_err(|e| format!("local udp recv failed: {e}"))?;
        let payload = buf[..n].to_vec();
        let st = stack.clone();
        let sock2 = sock.clone();
        tokio::spawn(async move {
            match st
                .udp_roundtrip(remote_ip, remote_port, &payload, Duration::from_secs(4))
                .await
            {
                Ok(resp) => {
                    if let Err(e) = sock2.send_to(&resp, from).await {
                        tracing::debug!("local udp send to {from} failed: {e}");
                    }
                }
                Err(e) => {
                    tracing::debug!("local udp roundtrip to [{}]:{} failed: {e}", remote_ip, remote_port);
                }
            }
        });
    }
}

pub async fn run_remote_udp_mapping(
    stack: Arc<SmolStack>,
    mapping: MappingSpec,
) -> Result<(), String> {
    let listen_port = mapping.first.port;
    let local_host = mapping
        .second
        .host
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let local_addr = format!("{local_host}:{}", mapping.second.port);
    let listener = stack.udp_bind_listener(listen_port).await?;

    struct UdpSession {
        sock: Arc<UdpSocket>,
        last_activity: Arc<Mutex<Instant>>,
    }

    const MAX_UDP_SESSIONS: usize = 4096;
    const UDP_SESSION_TTL: Duration = Duration::from_secs(120);

    let sessions: Arc<Mutex<HashMap<String, UdpSession>>> =
        Arc::new(Mutex::new(HashMap::new()));

    tracing::info!(
        "Mapping Yggdrasil UDP port {} -> local UDP {}",
        listen_port, local_addr
    );

    let mut last_cleanup = Instant::now();

    loop {
        let (payload, src_ip, src_port) = match stack
            .udp_recv_from(listener, Duration::from_secs(30))
            .await
        {
            Ok(v) => v,
            Err(e) if e.contains("timeout") => {
                // Periodic cleanup of stale sessions on timeout.
                let mut locked = sessions.lock().await;
                let before = locked.len();
                locked.retain(|_, s| {
                    let last = *s.last_activity.blocking_lock();
                    last.elapsed() < UDP_SESSION_TTL
                });
                let evicted = before - locked.len();
                if evicted > 0 {
                    tracing::debug!("remote-udp evicted {evicted} stale sessions, {} remaining", locked.len());
                }
                last_cleanup = Instant::now();
                continue;
            }
            Err(e) => return Err(e),
        };

        // Periodic cleanup on activity too.
        if last_cleanup.elapsed() > Duration::from_secs(30) {
            let mut locked = sessions.lock().await;
            let before = locked.len();
            locked.retain(|_, s| {
                let last = *s.last_activity.blocking_lock();
                last.elapsed() < UDP_SESSION_TTL
            });
            let evicted = before - locked.len();
            if evicted > 0 {
                tracing::debug!("remote-udp evicted {evicted} stale sessions, {} remaining", locked.len());
            }
            last_cleanup = Instant::now();
        }

        tracing::debug!(
            "remote-udp got {} bytes from ygg [{}]:{}",
            payload.len(),
            src_ip,
            src_port
        );
        let key = format!("[{}]:{}", src_ip, src_port);
        let local_sock = {
            let mut locked = sessions.lock().await;
            if let Some(s) = locked.get(&key) {
                *s.last_activity.lock().await = Instant::now();
                s.sock.clone()
            } else {
                if locked.len() >= MAX_UDP_SESSIONS {
                    tracing::warn!("remote-udp session limit reached ({MAX_UDP_SESSIONS}), dropping packet from {key}");
                    continue;
                }
                let s = Arc::new(
                    UdpSocket::bind("127.0.0.1:0")
                        .await
                        .map_err(|e| format!("bind local udp client socket: {e}"))?,
                );
                s.connect(&local_addr)
                    .await
                    .map_err(|e| format!("connect local udp client socket to {local_addr}: {e}"))?;

                let last_activity = Arc::new(Mutex::new(Instant::now()));

                let s_bg = s.clone();
                let st_bg = stack.clone();
                let local_addr_bg = local_addr.clone();
                let sessions_bg = sessions.clone();
                let key_bg = key.clone();
                let last_activity_bg = last_activity.clone();
                tokio::spawn(async move {
                    let mut resp = vec![0u8; 65535];
                    loop {
                        let n = match s_bg.recv(&mut resp).await {
                            Ok(n) => n,
                            Err(e) => {
                                tracing::debug!(
                                    "remote-udp session {} recv from local {} failed: {}",
                                    key_bg,
                                    local_addr_bg,
                                    e
                                );
                                sessions_bg.lock().await.remove(&key_bg);
                                return;
                            }
                        };
                        *last_activity_bg.lock().await = Instant::now();
                        tracing::debug!(
                            "remote-udp session {} got {} bytes from local {}, sending back to [{}]:{}",
                            key_bg,
                            n,
                            local_addr_bg,
                            src_ip,
                            src_port
                        );
                        if let Err(e) = st_bg.udp_send_to(listener, src_ip, src_port, &resp[..n], Duration::from_secs(4)).await {
                            tracing::debug!(
                                "remote-udp session {} send to ygg [{}]:{} failed: {}",
                                key_bg,
                                src_ip,
                                src_port,
                                e
                            );
                            sessions_bg.lock().await.remove(&key_bg);
                            return;
                        }
                    }
                });

                locked.insert(key.clone(), UdpSession { sock: s.clone(), last_activity });
                s
            }
        };

        local_sock
            .send(&payload)
            .await
            .map_err(|e| format!("remote-udp send to local service failed: {e}"))?;
        tracing::debug!(
            "remote-udp forwarded {} bytes to local service {} for session {}",
            payload.len(),
            local_addr,
            key
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_raw_ipv6() {
        let ip = resolve_target_host("200:abcd::1", None).await.expect("resolve");
        assert_eq!(ip, "200:abcd::1".parse::<Ipv6Addr>().unwrap());
    }

    #[tokio::test]
    async fn resolve_pk_ygg() {
        // Use a known 32-byte (64 hex char) key
        let key_hex = "0000000000000000000000000000000000000000000000000000000000000000";
        let host = format!("{key_hex}.pk.ygg");
        let result = resolve_target_host(&host, None).await;
        assert!(result.is_ok(), "should resolve .pk.ygg: {:?}", result);
    }

    #[tokio::test]
    async fn resolve_pk_ygg_with_subdomain() {
        let key_hex = "0000000000000000000000000000000000000000000000000000000000000000";
        let host = format!("sub.{key_hex}.pk.ygg");
        let result = resolve_target_host(&host, None).await;
        assert!(result.is_ok(), "should resolve subdomain.pk.ygg: {:?}", result);
    }

    #[tokio::test]
    async fn resolve_pk_ygg_invalid_hex() {
        let host = "not_hex_at_all_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.pk.ygg";
        assert!(resolve_target_host(host, None).await.is_err());
    }

    #[tokio::test]
    async fn resolve_pk_ygg_wrong_length() {
        // 31 bytes = 62 hex chars, should fail
        let host = "00000000000000000000000000000000000000000000000000000000000000.pk.ygg";
        let result = resolve_target_host(host, None).await;
        assert!(result.is_err(), "31-byte key should fail: {:?}", result);
    }
}
