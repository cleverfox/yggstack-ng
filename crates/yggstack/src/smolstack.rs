use std::collections::{HashMap, HashSet, VecDeque};
use std::net::Ipv6Addr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use smoltcp::iface::{Config as IfaceConfig, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, IpEndpoint, Ipv6Address};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Tracks a heap allocation that was leaked via `Box::into_raw` to satisfy
/// smoltcp's `'static` lifetime requirement.  Dropping the guard reclaims
/// the memory.
struct LeakedBuf {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: The raw pointer is only used to reconstruct a Box for deallocation.
unsafe impl Send for LeakedBuf {}

impl Drop for LeakedBuf {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.ptr, self.len));
        }
    }
}

/// Same as `LeakedBuf` but for `udp::PacketMetadata` slices.
struct LeakedMetaBuf {
    ptr: *mut udp::PacketMetadata,
    len: usize,
}

unsafe impl Send for LeakedMetaBuf {}

impl Drop for LeakedMetaBuf {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.ptr, self.len));
        }
    }
}

/// Holds all heap allocations associated with a single smoltcp socket so they
/// can be freed when the socket is removed from the `SocketSet`.
enum SocketBufs {
    Tcp {
        _rx: LeakedBuf,
        _tx: LeakedBuf,
    },
    Udp {
        _rx_meta: LeakedMetaBuf,
        _tx_meta: LeakedMetaBuf,
        _rx_data: LeakedBuf,
        _tx_data: LeakedBuf,
    },
}

/// Allocate a byte buffer, returning a `'static` mutable reference for smoltcp
/// and a `LeakedBuf` that will free the memory on drop.
fn alloc_buf(size: usize) -> (&'static mut [u8], LeakedBuf) {
    let boxed = vec![0u8; size].into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed) as *mut u8;
    // SAFETY: ptr is valid, aligned, and uniquely owned.
    let static_ref = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
    (static_ref, LeakedBuf { ptr, len })
}

/// Allocate a metadata buffer for UDP sockets.
fn alloc_meta_buf(count: usize) -> (&'static mut [udp::PacketMetadata], LeakedMetaBuf) {
    let boxed = vec![udp::PacketMetadata::EMPTY; count].into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed) as *mut udp::PacketMetadata;
    let static_ref = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
    (static_ref, LeakedMetaBuf { ptr, len })
}

use yggdrasil::core::Core;
use yggdrasil::ipv6rwc::ReadWriteCloser;

struct SharedQueues {
    inbound: Mutex<VecDeque<Vec<u8>>>,
    outbound: Mutex<VecDeque<Vec<u8>>>,
    notify: Notify,
    inbound_enqueued_pkts: AtomicU64,
    inbound_enqueued_bytes: AtomicU64,
    inbound_dequeued_pkts: AtomicU64,
    inbound_dequeued_bytes: AtomicU64,
    outbound_enqueued_pkts: AtomicU64,
    outbound_enqueued_bytes: AtomicU64,
    outbound_dequeued_pkts: AtomicU64,
    outbound_dequeued_bytes: AtomicU64,
}

impl SharedQueues {
    fn new() -> Self {
        Self {
            inbound: Mutex::new(VecDeque::new()),
            outbound: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            inbound_enqueued_pkts: AtomicU64::new(0),
            inbound_enqueued_bytes: AtomicU64::new(0),
            inbound_dequeued_pkts: AtomicU64::new(0),
            inbound_dequeued_bytes: AtomicU64::new(0),
            outbound_enqueued_pkts: AtomicU64::new(0),
            outbound_enqueued_bytes: AtomicU64::new(0),
            outbound_dequeued_pkts: AtomicU64::new(0),
            outbound_dequeued_bytes: AtomicU64::new(0),
        }
    }
}

fn sampled_counter_log(n: u64) -> bool {
    n <= 20 || n % 200 == 0
}

fn describe_ipv6_packet(packet: &[u8]) -> String {
    if packet.len() < 40 {
        return format!("len={} short", packet.len());
    }
    let next = packet[6];
    match next {
        6 => {
            if packet.len() >= 44 {
                let src = u16::from_be_bytes([packet[40], packet[41]]);
                let dst = u16::from_be_bytes([packet[42], packet[43]]);
                format!("len={} tcp {}->{}", packet.len(), src, dst)
            } else {
                format!("len={} tcp short", packet.len())
            }
        }
        17 => {
            if packet.len() >= 48 {
                let src = u16::from_be_bytes([packet[40], packet[41]]);
                let dst = u16::from_be_bytes([packet[42], packet[43]]);
                format!("len={} udp {}->{}", packet.len(), src, dst)
            } else {
                format!("len={} udp short", packet.len())
            }
        }
        58 => format!("len={} icmpv6", packet.len()),
        _ => format!("len={} nh={}", packet.len(), next),
    }
}

struct YggDevice {
    queues: Arc<SharedQueues>,
    mtu: usize,
}

struct YggRxToken {
    data: Vec<u8>,
}

struct YggTxToken {
    queues: Arc<SharedQueues>,
    mtu: usize,
}

impl RxToken for YggRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.data)
    }
}

impl TxToken for YggTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut packet = vec![0u8; len.min(self.mtu)];
        let out = f(&mut packet);
        let pkt_len = packet.len();
        self.queues.outbound.lock().expect("outbound queue poisoned").push_back(packet);
        let pkt = self
            .queues
            .outbound_enqueued_pkts
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let bytes = self
            .queues
            .outbound_enqueued_bytes
            .fetch_add(pkt_len as u64, Ordering::Relaxed)
            + pkt_len as u64;
        if sampled_counter_log(pkt) {
            tracing::debug!(
                "yggdevice tx enqueue pkt={} len={} total_bytes={}",
                pkt,
                pkt_len,
                bytes
            );
        }
        self.queues.notify.notify_waiters();
        out
    }
}

impl Device for YggDevice {
    type RxToken<'a> = YggRxToken;
    type TxToken<'a> = YggTxToken;

    fn receive(&mut self, _timestamp: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let data = self.queues.inbound.lock().expect("inbound queue poisoned").pop_front()?;
        let pkt_len = data.len();
        let pkt = self
            .queues
            .inbound_dequeued_pkts
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let bytes = self
            .queues
            .inbound_dequeued_bytes
            .fetch_add(pkt_len as u64, Ordering::Relaxed)
            + pkt_len as u64;
        if sampled_counter_log(pkt) {
            tracing::debug!(
                "yggdevice rx dequeue pkt={} len={} total_bytes={}",
                pkt,
                pkt_len,
                bytes
            );
        }
        Some((
            YggRxToken { data },
            YggTxToken {
                queues: self.queues.clone(),
                mtu: self.mtu,
            },
        ))
    }

    fn transmit(&mut self, _timestamp: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(YggTxToken {
            queues: self.queues.clone(),
            mtu: self.mtu,
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.max_transmission_unit = self.mtu;
        caps.medium = Medium::Ip;
        caps
    }
}

struct StackState {
    iface: Interface,
    sockets: SocketSet<'static>,
    device: YggDevice,
    next_ephemeral_port: u16,
    /// Tracks heap allocations per socket handle so they can be freed on removal.
    socket_bufs: HashMap<SocketHandle, SocketBufs>,
    /// Ephemeral ports currently in use.
    used_ports: HashSet<u16>,
    /// Maps socket handles to their ephemeral port for cleanup.
    handle_ports: HashMap<SocketHandle, u16>,
}

const EPHEMERAL_PORT_START: u16 = 40000;
const EPHEMERAL_PORT_END: u16 = 65000;

impl StackState {
    /// Remove a socket and free its associated buffers and ephemeral port.
    fn remove_socket(&mut self, handle: SocketHandle) {
        self.sockets.remove(handle);
        self.socket_bufs.remove(&handle);
        if let Some(port) = self.handle_ports.remove(&handle) {
            self.used_ports.remove(&port);
        }
    }

    /// Allocate an unused ephemeral port, returning an error if all ports are exhausted.
    fn allocate_ephemeral_port(&mut self, handle: SocketHandle) -> Result<u16, String> {
        let range_size = (EPHEMERAL_PORT_END - EPHEMERAL_PORT_START) as usize;
        for _ in 0..range_size {
            // Wrap before use to ensure port stays in [START, END).
            if self.next_ephemeral_port >= EPHEMERAL_PORT_END {
                self.next_ephemeral_port = EPHEMERAL_PORT_START;
            }
            let port = self.next_ephemeral_port;
            self.next_ephemeral_port += 1;
            if !self.used_ports.contains(&port) {
                self.used_ports.insert(port);
                self.handle_ports.insert(handle, port);
                return Ok(port);
            }
        }
        Err("no ephemeral ports available".to_string())
    }
}

struct TcpProxySessionState {
    id: u64,
    started: Instant,
    last_activity: Instant,
    bytes_from_local: u64,
    bytes_from_remote: u64,
}

pub struct SmolStack {
    queues: Arc<SharedQueues>,
    _rwc: Arc<ReadWriteCloser>,
    state: tokio::sync::Mutex<StackState>,
    next_session_id: AtomicU64,
}

impl SmolStack {
    pub fn new(core: Arc<Core>) -> Arc<Self> {
        let mtu = core.mtu() as usize;
        let rwc = ReadWriteCloser::new(core.clone(), core.mtu(), None);
        core.set_path_notify(rwc.clone());

        let queues = Arc::new(SharedQueues::new());
        let device = YggDevice {
            queues: queues.clone(),
            mtu,
        };

        let mut if_cfg = IfaceConfig::new(HardwareAddress::Ip);
        if_cfg.random_seed = rand::random::<u64>();
        let mut iface = Interface::new(if_cfg, &mut YggDevice {
            queues: queues.clone(),
            mtu,
        }, SmolInstant::from_millis(0));

        // Yggdrasil addresses are in 0200::/7. Assigning /7 allows on-link routing.
        let local_ip = Ipv6Address::from_octets(core.address().0);
        iface.update_ip_addrs(|addrs| {
            let _ = addrs.push(IpCidr::new(IpAddress::Ipv6(local_ip), 7));
        });

        let state = StackState {
            iface,
            sockets: SocketSet::new(vec![]),
            device,
            next_ephemeral_port: EPHEMERAL_PORT_START,
            socket_bufs: HashMap::new(),
            used_ports: HashSet::new(),
            handle_ports: HashMap::new(),
        };

        let stack = Arc::new(Self {
            queues,
            _rwc: rwc.clone(),
            state: tokio::sync::Mutex::new(state),
            next_session_id: AtomicU64::new(1),
        });

        let reader = stack.clone();
        tokio::spawn(async move {
            reader.read_loop(rwc).await;
        });

        let writer = stack.clone();
        tokio::spawn(async move {
            writer.write_loop().await;
        });

        stack
    }

    async fn read_loop(&self, rwc: Arc<ReadWriteCloser>) {
        loop {
            let mut buf = vec![0u8; rwc.mtu() as usize];
            match rwc.read(&mut buf).await {
                Ok(n) if n > 0 => {
                    let packet_desc = describe_ipv6_packet(&buf[..n]);
                    buf.truncate(n);
                    self.queues.inbound.lock().expect("inbound queue poisoned").push_back(buf);
                    let pkt = self
                        .queues
                        .inbound_enqueued_pkts
                        .fetch_add(1, Ordering::Relaxed)
                        + 1;
                    let bytes = self
                        .queues
                        .inbound_enqueued_bytes
                        .fetch_add(n as u64, Ordering::Relaxed)
                        + n as u64;
                    let always_log = packet_desc.contains(" udp ");
                    if always_log || sampled_counter_log(pkt) {
                        tracing::debug!(
                            "rwc->inbound enqueue pkt={} len={} total_bytes={} {}",
                            pkt,
                            n,
                            bytes,
                            packet_desc
                        );
                    }
                    self.queues.notify.notify_waiters();
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("smolstack read loop error: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    async fn write_loop(&self) {
        loop {
            let packet = self.queues.outbound.lock().expect("outbound queue poisoned").pop_front();

            if let Some(packet) = packet {
                let pkt_len = packet.len();
                let pkt = self
                    .queues
                    .outbound_dequeued_pkts
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                let bytes = self
                    .queues
                    .outbound_dequeued_bytes
                    .fetch_add(pkt_len as u64, Ordering::Relaxed)
                    + pkt_len as u64;
                if sampled_counter_log(pkt) {
                    tracing::debug!(
                        "outbound->rwc dequeue pkt={} len={} total_bytes={}",
                        pkt,
                        pkt_len,
                        bytes
                    );
                }
                if let Err(e) = self._rwc.write(&packet).await {
                    tracing::debug!("smolstack write failed: {e}");
                }
                continue;
            }

            self.queues.notify.notified().await;
        }
    }

    fn now() -> SmolInstant {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        SmolInstant::from_millis(ms)
    }

    pub async fn tcp_probe_http(
        &self,
        remote: Ipv6Addr,
        port: u16,
        host_header: &str,
        timeout: Duration,
    ) -> Result<String, String> {
        let (rx_buf, rx_guard) = alloc_buf(128 * 1024);
        let (tx_buf, tx_guard) = alloc_buf(128 * 1024);
        let socket = tcp::Socket::new(
            tcp::SocketBuffer::new(rx_buf),
            tcp::SocketBuffer::new(tx_buf),
        );

        let handle = {
            let mut st = self.state.lock().await;
            let h = st.sockets.add(socket);
            st.socket_bufs.insert(h, SocketBufs::Tcp { _rx: rx_guard, _tx: tx_guard });
            let local_port = st.allocate_ephemeral_port(h)?;

            let endpoint = (IpAddress::Ipv6(Ipv6Address::from_octets(remote.octets())), port);
            let StackState { iface, sockets, .. } = &mut *st;
            let s = sockets.get_mut::<tcp::Socket>(h);
            s.connect(iface.context(), endpoint, local_port)
                .map_err(|e| format!("smoltcp connect failed: {e:?}"))?;
            h
        };

        let started = Instant::now();
        let request = format!(
            "GET / HTTP/1.1\r\nHost: {host_header}\r\nUser-Agent: yggstack-smolprobe\r\nConnection: close\r\n\r\n"
        );
        let mut sent = false;
        let mut resp = Vec::new();

        while started.elapsed() < timeout {
            let mut done = false;
            {
                let mut st = self.state.lock().await;
                let now = Self::now();
                let StackState {
                    iface,
                    sockets,
                    device,
                    ..
                } = &mut *st;
                let _ = iface.poll(now, device, sockets);

                let s = sockets.get_mut::<tcp::Socket>(handle);
                if s.may_send() && !sent {
                    if s.send_slice(request.as_bytes()).is_ok() {
                        sent = true;
                    }
                }

                while s.can_recv() {
                    let _ = s.recv(|data| {
                        resp.extend_from_slice(data);
                        (data.len(), ())
                    });
                }

                if sent && (!s.is_active() || !s.may_recv()) {
                    done = true;
                }
            }

            if done && !resp.is_empty() {
                let mut st = self.state.lock().await;
                st.remove_socket(handle);
                return String::from_utf8(resp).map_err(|e| format!("response is not valid UTF-8: {e}"));
            }

            tokio::select! {
                _ = self.queues.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(20)) => {},
            }
        }

        let mut st = self.state.lock().await;
        st.remove_socket(handle);
        Err("tcp probe timeout".to_string())
    }

    pub async fn proxy_tcp_tokio<S>(
        &self,
        local: S,
        remote: Ipv6Addr,
        port: u16,
    ) -> Result<(), String>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (rx_buf, rx_guard) = alloc_buf(128 * 1024);
        let (tx_buf, tx_guard) = alloc_buf(128 * 1024);
        let socket = tcp::Socket::new(
            tcp::SocketBuffer::new(rx_buf),
            tcp::SocketBuffer::new(tx_buf),
        );

        let handle = {
            let mut st = self.state.lock().await;
            let h = st.sockets.add(socket);
            st.socket_bufs.insert(h, SocketBufs::Tcp { _rx: rx_guard, _tx: tx_guard });
            let local_port = st.allocate_ephemeral_port(h)?;
            let endpoint = (IpAddress::Ipv6(Ipv6Address::from_octets(remote.octets())), port);
            let StackState { iface, sockets, .. } = &mut *st;
            let s = sockets.get_mut::<tcp::Socket>(h);
            s.connect(iface.context(), endpoint, local_port)
                .map_err(|e| format!("smoltcp connect failed: {e:?}"))?;
            h
        };

        let session = TcpProxySessionState {
            id: self.next_session_id.fetch_add(1, Ordering::Relaxed),
            started: Instant::now(),
            last_activity: Instant::now(),
            bytes_from_local: 0,
            bytes_from_remote: 0,
        };
        self.proxy_tcp_existing_handle_tokio(local, handle, session).await
    }

    async fn proxy_tcp_existing_handle_tokio<S>(
        &self,
        mut local: S,
        handle: SocketHandle,
        mut session: TcpProxySessionState,
    ) -> Result<(), String>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let to_remote = &mut Vec::with_capacity(8192);
        let mut from_remote = Vec::with_capacity(8192);
        let mut local_buf = vec![0u8; 8192];
        let mut local_closed = false;
        let mut remote_closed = false;

        loop {
            tokio::select! {
                read_res = local.read(&mut local_buf), if !local_closed => {
                    match read_res {
                        Ok(0) => local_closed = true,
                        Ok(n) => {
                            tracing::debug!("tcp session={} read {} bytes from local backend", session.id, n);
                            to_remote.extend_from_slice(&local_buf[..n]);
                            session.bytes_from_local += n as u64;
                            session.last_activity = Instant::now();
                        }
                        Err(e) => {
                            let mut st = self.state.lock().await;
                            st.remove_socket(handle);
                            return Err(format!("session {} local read failed: {e}", session.id));
                        }
                    }
                }
                _ = self.queues.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }

            {
                let mut st = self.state.lock().await;
                let now = Self::now();
                let StackState {
                    iface,
                    sockets,
                    device,
                    ..
                } = &mut *st;
                let _ = iface.poll(now, device, sockets);

                let s = sockets.get_mut::<tcp::Socket>(handle);

                if !to_remote.is_empty() && s.can_send() {
                    let max = to_remote.len().min(4096);
                    if let Ok(n) = s.send_slice(&to_remote[..max]) {
                        tracing::debug!("tcp session={} sent {} bytes to ygg peer", session.id, n);
                        to_remote.drain(..n);
                        session.last_activity = Instant::now();
                    }
                }

                while s.can_recv() {
                    let _ = s.recv(|data| {
                        tracing::debug!("tcp session={} received {} bytes from ygg peer", session.id, data.len());
                        from_remote.extend_from_slice(data);
                        session.bytes_from_remote += data.len() as u64;
                        session.last_activity = Instant::now();
                        (data.len(), ())
                    });
                }

                if local_closed && s.may_send() {
                    s.close();
                }
                if !s.is_active() && !s.may_recv() && to_remote.is_empty() {
                    remote_closed = true;
                }

                // Flush any pending outbound packets generated by send_slice/close
                let _ = iface.poll(now, device, sockets);
            }

            if !from_remote.is_empty() {
                tracing::debug!("tcp session={} writing {} bytes to local backend", session.id, from_remote.len());
                local
                    .write_all(&from_remote)
                    .await
                    .map_err(|e| format!("session {} local write failed: {e}", session.id))?;
                from_remote.clear();
                session.last_activity = Instant::now();
            }

            if local_closed && remote_closed {
                let mut st = self.state.lock().await;
                st.remove_socket(handle);
                tracing::debug!(
                    "tcp session={} completed duration_ms={} tx={} rx={}",
                    session.id,
                    session.started.elapsed().as_millis(),
                    session.bytes_from_local,
                    session.bytes_from_remote
                );
                return Ok(());
            }

            // If one side has closed and no new activity happened for a while,
            // force-close the smoltcp socket to avoid stale hung sessions.
            if (local_closed || remote_closed)
                && to_remote.is_empty()
                && from_remote.is_empty()
                && session.last_activity.elapsed() > Duration::from_secs(3)
            {
                let mut st = self.state.lock().await;
                let s = st.sockets.get_mut::<tcp::Socket>(handle);
                if s.is_open() {
                    s.abort();
                }
                st.remove_socket(handle);
                tracing::debug!(
                    "tcp session={} forced close after idle duration_ms={} tx={} rx={}",
                    session.id,
                    session.started.elapsed().as_millis(),
                    session.bytes_from_local,
                    session.bytes_from_remote
                );
                return Ok(());
            }

            // Do not let silent half-open sessions block remote listener progress forever.
            if to_remote.is_empty()
                && from_remote.is_empty()
                && session.last_activity.elapsed() > Duration::from_secs(8)
            {
                let mut st = self.state.lock().await;
                let s = st.sockets.get_mut::<tcp::Socket>(handle);
                if s.is_open() {
                    s.abort();
                }
                st.remove_socket(handle);
                tracing::debug!(
                    "tcp session={} closed on idle timeout duration_ms={} tx={} rx={}",
                    session.id,
                    session.started.elapsed().as_millis(),
                    session.bytes_from_local,
                    session.bytes_from_remote
                );
                return Ok(());
            }

        }
    }

    async fn create_remote_tcp_listener_handle(&self, listen_port: u16) -> Result<SocketHandle, String> {
        let (rx_buf, rx_guard) = alloc_buf(128 * 1024);
        let (tx_buf, tx_guard) = alloc_buf(128 * 1024);
        let socket = tcp::Socket::new(
            tcp::SocketBuffer::new(rx_buf),
            tcp::SocketBuffer::new(tx_buf),
        );
        let mut st = self.state.lock().await;
        let h = st.sockets.add(socket);
        st.socket_bufs.insert(h, SocketBufs::Tcp { _rx: rx_guard, _tx: tx_guard });
        let s = st.sockets.get_mut::<tcp::Socket>(h);
        s.listen(listen_port)
            .map_err(|e| format!("smoltcp listen failed on port {listen_port}: {e:?}"))?;
        Ok(h)
    }

    async fn wait_remote_tcp_accept(&self, handle: SocketHandle, listen_port: u16) -> Result<(), String> {
        loop {
            {
                let mut st = self.state.lock().await;
                let now = Self::now();
                let StackState {
                    iface,
                    sockets,
                    device,
                    ..
                } = &mut *st;
                let _ = iface.poll(now, device, sockets);
                let s = sockets.get_mut::<tcp::Socket>(handle);
                if s.is_active() {
                    tracing::debug!("remote-tcp accepted connection on ygg port {}", listen_port);
                    return Ok(());
                }
            }
            tokio::select! {
                _ = self.queues.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }

    pub async fn run_remote_tcp_listener_forward(
        self: Arc<Self>,
        listen_port: u16,
        local_target: &str,
    ) -> Result<(), String> {
        let local_target = local_target.to_string();
        let mut listener = self.create_remote_tcp_listener_handle(listen_port).await?;
        tracing::debug!("remote-tcp listening on ygg port {}", listen_port);

        loop {
            self.wait_remote_tcp_accept(listener, listen_port).await?;
            let accepted = listener;
            let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
            let local = match TcpStream::connect(&local_target).await {
                Ok(s) => s,
                Err(e) => {
                    let mut locked = self.state.lock().await;
                    locked.remove_socket(accepted);
                    tracing::warn!(
                        "remote-tcp session={} failed to connect to local target {}: {}",
                        session_id,
                        local_target,
                        e
                    );
                    listener = self.create_remote_tcp_listener_handle(listen_port).await?;
                    continue;
                }
            };

            tracing::debug!(
                "remote-tcp session={} connected local backend {}",
                session_id,
                local_target
            );
            let session = TcpProxySessionState {
                id: session_id,
                started: Instant::now(),
                last_activity: Instant::now(),
                bytes_from_local: 0,
                bytes_from_remote: 0,
            };

            let stack = self.clone();
            tokio::spawn(async move {
                if let Err(e) = stack.proxy_tcp_existing_handle_tokio(local, accepted, session).await {
                    tracing::debug!("remote-tcp session={} ended with error: {}", session_id, e);
                }
            });

            listener = self.create_remote_tcp_listener_handle(listen_port).await?;
        }
    }

    pub async fn udp_roundtrip(
        &self,
        remote: Ipv6Addr,
        remote_port: u16,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        let (rx_meta, rx_meta_guard) = alloc_meta_buf(16);
        let (tx_meta, tx_meta_guard) = alloc_meta_buf(16);
        let (rx_data, rx_data_guard) = alloc_buf(65535);
        let (tx_data, tx_data_guard) = alloc_buf(65535);

        let socket = udp::Socket::new(
            udp::PacketBuffer::new(rx_meta, rx_data),
            udp::PacketBuffer::new(tx_meta, tx_data),
        );

        let handle = {
            let mut st = self.state.lock().await;
            let h = st.sockets.add(socket);
            st.socket_bufs.insert(h, SocketBufs::Udp {
                _rx_meta: rx_meta_guard,
                _tx_meta: tx_meta_guard,
                _rx_data: rx_data_guard,
                _tx_data: tx_data_guard,
            });
            let local_port = st.allocate_ephemeral_port(h)?;
            let s = st.sockets.get_mut::<udp::Socket>(h);
            s.bind(local_port)
                .map_err(|e| format!("smoltcp udp bind failed: {e:?}"))?;
            h
        };

        let endpoint = IpEndpoint::new(IpAddress::Ipv6(Ipv6Address::from_octets(remote.octets())), remote_port);
        let started = Instant::now();
        let mut sent = false;
        let mut out = vec![0u8; 65535];

        loop {
            if started.elapsed() > timeout {
                let mut st = self.state.lock().await;
                st.remove_socket(handle);
                return Err("udp roundtrip timeout".to_string());
            }

            {
                let mut st = self.state.lock().await;
                let now = Self::now();
                {
                    let StackState {
                        iface,
                        sockets,
                        device,
                        ..
                    } = &mut *st;
                    let _ = iface.poll(now, device, sockets);
                }

                let s = st.sockets.get_mut::<udp::Socket>(handle);
                if !sent && s.can_send() {
                    s.send_slice(payload, endpoint)
                        .map_err(|e| format!("smoltcp udp send failed: {e:?}"))?;
                    sent = true;
                }
                if s.can_recv() {
                    let (n, _) = s
                        .recv_slice(&mut out)
                        .map_err(|e| format!("smoltcp udp recv failed: {e:?}"))?;
                    out.truncate(n);
                    st.remove_socket(handle);
                    return Ok(out);
                }
            }

            tokio::select! {
                _ = self.queues.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }

    pub async fn udp_bind_listener(&self, port: u16) -> Result<SocketHandle, String> {
        let (rx_meta, rx_meta_guard) = alloc_meta_buf(64);
        let (tx_meta, tx_meta_guard) = alloc_meta_buf(64);
        let (rx_data, rx_data_guard) = alloc_buf(65535);
        let (tx_data, tx_data_guard) = alloc_buf(65535);
        let socket = udp::Socket::new(
            udp::PacketBuffer::new(rx_meta, rx_data),
            udp::PacketBuffer::new(tx_meta, tx_data),
        );

        let mut st = self.state.lock().await;
        let h = st.sockets.add(socket);
        st.socket_bufs.insert(h, SocketBufs::Udp {
            _rx_meta: rx_meta_guard,
            _tx_meta: tx_meta_guard,
            _rx_data: rx_data_guard,
            _tx_data: tx_data_guard,
        });
        let s = st.sockets.get_mut::<udp::Socket>(h);
        s.bind(port)
            .map_err(|e| format!("smoltcp udp listen bind failed on {port}: {e:?}"))?;
        Ok(h)
    }

    pub async fn udp_recv_from(
        &self,
        handle: SocketHandle,
        timeout: Duration,
    ) -> Result<(Vec<u8>, Ipv6Addr, u16), String> {
        let started = Instant::now();
        let mut out = vec![0u8; 65535];
        loop {
            if started.elapsed() > timeout {
                return Err("udp recv timeout".to_string());
            }
            {
                let mut st = self.state.lock().await;
                let now = Self::now();
                let StackState {
                    iface,
                    sockets,
                    device,
                    ..
                } = &mut *st;
                let _ = iface.poll(now, device, sockets);
                let s = sockets.get_mut::<udp::Socket>(handle);
                if s.can_recv() {
                    let (n, ep) = s
                        .recv_slice(&mut out)
                        .map_err(|e| format!("smoltcp udp recv failed: {e:?}"))?;
                    let IpAddress::Ipv6(v6) = ep.endpoint.addr;
                    let ip = Ipv6Addr::from(v6.octets());
                    tracing::debug!(
                        "udp listener handle={} received {} bytes from [{}]:{}",
                        handle,
                        n,
                        ip,
                        ep.endpoint.port
                    );
                    out.truncate(n);
                    return Ok((out, ip, ep.endpoint.port));
                }
            }
            tokio::select! {
                _ = self.queues.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }

    pub async fn udp_send_to(
        &self,
        handle: SocketHandle,
        remote: Ipv6Addr,
        remote_port: u16,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<(), String> {
        let endpoint = IpEndpoint::new(IpAddress::Ipv6(Ipv6Address::from_octets(remote.octets())), remote_port);
        let started = Instant::now();
        loop {
            if started.elapsed() > timeout {
                return Err("udp send timeout".to_string());
            }
            {
                let mut st = self.state.lock().await;
                let now = Self::now();
                let StackState {
                    iface,
                    sockets,
                    device,
                    ..
                } = &mut *st;
                let _ = iface.poll(now, device, sockets);
                let s = sockets.get_mut::<udp::Socket>(handle);
                if s.can_send() {
                    s.send_slice(payload, endpoint)
                        .map_err(|e| format!("smoltcp udp send failed: {e:?}"))?;
                    tracing::debug!(
                        "udp listener handle={} sent {} bytes to [{}]:{}",
                        handle,
                        payload.len(),
                        remote,
                        remote_port
                    );
                    return Ok(());
                }
            }
            tokio::select! {
                _ = self.queues.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_buf_returns_correct_size() {
        let (buf, _guard) = alloc_buf(1024);
        assert_eq!(buf.len(), 1024);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn leaked_buf_drop_does_not_panic() {
        let (_buf, guard) = alloc_buf(4096);
        drop(guard);
    }

    #[test]
    fn alloc_meta_buf_returns_correct_count() {
        let (buf, _guard) = alloc_meta_buf(16);
        assert_eq!(buf.len(), 16);
    }

    fn make_test_stack_state() -> StackState {
        let queues = Arc::new(SharedQueues::new());
        let mtu = 1500;
        let mut device = YggDevice {
            queues: queues.clone(),
            mtu,
        };
        let if_cfg = IfaceConfig::new(HardwareAddress::Ip);
        let iface = Interface::new(if_cfg, &mut device, SmolInstant::from_millis(0));

        StackState {
            iface,
            sockets: SocketSet::new(vec![]),
            device,
            next_ephemeral_port: EPHEMERAL_PORT_START,
            socket_bufs: HashMap::new(),
            used_ports: HashSet::new(),
            handle_ports: HashMap::new(),
        }
    }

    #[test]
    fn ephemeral_port_allocation() {
        let mut st = make_test_stack_state();

        let (rx, rx_g) = alloc_buf(1024);
        let (tx, tx_g) = alloc_buf(1024);
        let sock = tcp::Socket::new(
            tcp::SocketBuffer::new(rx),
            tcp::SocketBuffer::new(tx),
        );
        let h1 = st.sockets.add(sock);
        st.socket_bufs.insert(h1, SocketBufs::Tcp { _rx: rx_g, _tx: tx_g });

        let port1 = st.allocate_ephemeral_port(h1).expect("first alloc");
        assert_eq!(port1, EPHEMERAL_PORT_START);

        let (rx, rx_g) = alloc_buf(1024);
        let (tx, tx_g) = alloc_buf(1024);
        let sock = tcp::Socket::new(
            tcp::SocketBuffer::new(rx),
            tcp::SocketBuffer::new(tx),
        );
        let h2 = st.sockets.add(sock);
        st.socket_bufs.insert(h2, SocketBufs::Tcp { _rx: rx_g, _tx: tx_g });

        let port2 = st.allocate_ephemeral_port(h2).expect("second alloc");
        assert_eq!(port2, EPHEMERAL_PORT_START + 1);
        assert_ne!(port1, port2);
    }

    #[test]
    fn remove_socket_frees_port() {
        let mut st = make_test_stack_state();

        let (rx, rx_g) = alloc_buf(1024);
        let (tx, tx_g) = alloc_buf(1024);
        let sock = tcp::Socket::new(
            tcp::SocketBuffer::new(rx),
            tcp::SocketBuffer::new(tx),
        );
        let h = st.sockets.add(sock);
        st.socket_bufs.insert(h, SocketBufs::Tcp { _rx: rx_g, _tx: tx_g });

        let port = st.allocate_ephemeral_port(h).expect("alloc");
        assert!(st.used_ports.contains(&port));

        st.remove_socket(h);
        assert!(!st.used_ports.contains(&port));
        assert!(!st.handle_ports.contains_key(&h));
        assert!(!st.socket_bufs.contains_key(&h));
    }

    #[test]
    fn ephemeral_port_exhaustion() {
        let mut st = make_test_stack_state();

        // Simulate exhaustion by pre-filling used_ports with all but 1 port
        for p in EPHEMERAL_PORT_START..EPHEMERAL_PORT_END {
            st.used_ports.insert(p);
        }

        // All ports occupied — allocation should fail
        let (rx, rx_g) = alloc_buf(64);
        let (tx, tx_g) = alloc_buf(64);
        let sock = tcp::Socket::new(
            tcp::SocketBuffer::new(rx),
            tcp::SocketBuffer::new(tx),
        );
        let h = st.sockets.add(sock);
        st.socket_bufs.insert(h, SocketBufs::Tcp { _rx: rx_g, _tx: tx_g });
        assert!(st.allocate_ephemeral_port(h).is_err());

        // Free one port and try again
        st.used_ports.remove(&(EPHEMERAL_PORT_START + 5));
        let (rx, rx_g) = alloc_buf(64);
        let (tx, tx_g) = alloc_buf(64);
        let sock = tcp::Socket::new(
            tcp::SocketBuffer::new(rx),
            tcp::SocketBuffer::new(tx),
        );
        let h2 = st.sockets.add(sock);
        st.socket_bufs.insert(h2, SocketBufs::Tcp { _rx: rx_g, _tx: tx_g });
        let port = st.allocate_ephemeral_port(h2).expect("should succeed after freeing one");
        assert_eq!(port, EPHEMERAL_PORT_START + 5);
    }
}
