use std::fs::File;
use std::io::Read;

use ed25519_dalek::SigningKey;
use getopts::Options;
use time::macros::format_description;
use tracing_subscriber::{fmt, EnvFilter};

mod compat_config;
mod mapping;
mod runtime;
mod smolstack;
mod socks;

use compat_config::{parse_compat_config, CompatConfig};
use mapping::{parse_local_mapping, parse_remote_mapping};
use runtime::{
    run_local_tcp_mapping, run_local_udp_mapping, run_remote_tcp_mapping, run_remote_udp_mapping,
    run_socks_server,
};
use smolstack::SmolStack;
use socks::parse_socks_listen;
use yggdrasil::address::{addr_for_key, subnet_for_key};
use yggdrasil::admin::AdminSocket;
use yggdrasil::core::Core;

fn read_compat_config_from_file(path: &str) -> Result<CompatConfig, String> {
    let mut f = File::open(path).map_err(|e| format!("open config file: {e}"))?;
    let mut s = String::new();
    f.read_to_string(&mut s)
        .map_err(|e| format!("read config file: {e}"))?;

    parse_compat_config(&s)
}

fn read_compat_config_from_stdin() -> Result<CompatConfig, String> {
    let mut s = String::new();
    std::io::stdin()
        .read_to_string(&mut s)
        .map_err(|e| format!("read stdin config: {e}"))?;

    parse_compat_config(&s)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let mut opts = Options::new();
    opts.optflag("", "autoconf", "automatic mode (ephemeral keys, no TUN)");
    opts.optflag("", "useconf", "read JSON/HJSON-style config from stdin");
    opts.optopt("", "useconffile", "read JSON/HJSON-style config from file path", "FILE");
    opts.optflag("", "genconf", "print a new config to stdout");
    opts.optflag("", "json", "print config as JSON (default currently JSON)");
    opts.optflag("", "normaliseconf", "print normalized JSON config and exit");
    opts.optflag("", "address", "print IPv6 address from config and exit");
    opts.optflag("", "subnet", "print IPv6 subnet from config and exit");
    opts.optflag("", "publickey", "print public key from config and exit");
    opts.optopt("", "loglevel", "log level: error,warn,info,debug,trace", "LEVEL");
    opts.optmulti("", "local-tcp", "TCP local mapping", "SPEC");
    opts.optmulti("", "local-udp", "UDP local mapping", "SPEC");
    opts.optmulti("", "remote-tcp", "TCP remote mapping", "SPEC");
    opts.optmulti("", "remote-udp", "UDP remote mapping", "SPEC");
    opts.optopt("", "socks", "SOCKS listener address (runtime pending)", "ADDR");
    opts.optopt("", "nameserver", "Yggdrasil IPv6 DNS server for SOCKS (runtime pending)", "ADDR");
    opts.optopt("", "probe-tcp", "probe remote Yggdrasil TCP endpoint [ipv6]:port using smoltcp", "ENDPOINT");
    opts.optflag("", "version", "print version");
    opts.optflag("h", "help", "print help");

    let m = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Error: {e}");
            eprintln!("{}", opts.usage("Usage: yggstack [options]"));
            std::process::exit(1);
        }
    };

    if m.opt_present("help") {
        println!("{}", opts.usage("Usage: yggstack [options]"));
        return Ok(());
    }

    if m.opt_present("version") {
        println!("yggstack {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    if m.opt_present("genconf") {
        let cfg = CompatConfig::default();
        if !m.opt_present("json") {
            eprintln!("WARNING: HJSON output is not implemented yet, falling back to JSON");
        }
        println!("{}", serde_json::to_string_pretty(&cfg)?);
        return Ok(());
    }

    let loglevel = m.opt_str("loglevel").unwrap_or_else(|| "info".to_string());
    let filter = EnvFilter::try_new(&loglevel).unwrap_or_else(|_| EnvFilter::new("info"));
    let format = format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]");
    let timer = fmt::time::LocalTime::new(format);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_level(true)
        .with_timer(timer)
        .init();

    let compat_cfg = if m.opt_present("autoconf") {
        CompatConfig::default()
    } else if m.opt_present("useconf") {
        read_compat_config_from_stdin().map_err(|e| format!("{e}"))?
    } else if let Some(path) = m.opt_str("useconffile") {
        read_compat_config_from_file(&path).map_err(|e| format!("{e}"))?
    } else {
        eprintln!("Specify one of: --autoconf, --useconf, --useconffile");
        std::process::exit(1);
    };

    if m.opt_present("normaliseconf") {
        if !m.opt_present("json") {
            tracing::warn!("HJSON output is not implemented yet, falling back to JSON");
        }
        println!("{}", serde_json::to_string_pretty(&compat_cfg)?);
        return Ok(());
    }

    let local_tcp = m.opt_strs("local-tcp");
    let local_udp = m.opt_strs("local-udp");
    let remote_tcp = m.opt_strs("remote-tcp");
    let remote_udp = m.opt_strs("remote-udp");
    let socks_addr = m.opt_str("socks");
    let nameserver = m.opt_str("nameserver");
    let probe_tcp = m.opt_str("probe-tcp");

    let mut local_tcp_mappings = Vec::new();
    for spec in &local_tcp {
        let parsed = parse_local_mapping(spec)
            .map_err(|e| format!("invalid --local-tcp mapping '{spec}': {e}"))?;
        local_tcp_mappings.push(parsed);
    }
    let mut local_udp_mappings = Vec::new();
    for spec in &local_udp {
        let parsed = parse_local_mapping(spec)
            .map_err(|e| format!("invalid --local-udp mapping '{spec}': {e}"))?;
        local_udp_mappings.push(parsed);
    }
    let mut remote_tcp_mappings = Vec::new();
    for spec in &remote_tcp {
        let parsed = parse_remote_mapping(spec)
            .map_err(|e| format!("invalid --remote-tcp mapping '{spec}': {e}"))?;
        remote_tcp_mappings.push(parsed);
    }
    let mut remote_udp_mappings = Vec::new();
    for spec in &remote_udp {
        let parsed = parse_remote_mapping(spec)
            .map_err(|e| format!("invalid --remote-udp mapping '{spec}': {e}"))?;
        remote_udp_mappings.push(parsed);
    }

    if !(local_tcp.is_empty() && local_udp.is_empty() && remote_tcp.is_empty() && remote_udp.is_empty()) {
        tracing::info!(
            "Validated mappings: local-tcp={}, local-udp={}, remote-tcp={}, remote-udp={}",
            local_tcp.len(),
            local_udp.len(),
            remote_tcp.len(),
            remote_udp.len()
        );
    }
    let socks_listen = if let Some(addr) = socks_addr {
        let listen =
            parse_socks_listen(&addr).map_err(|e| format!("invalid --socks address '{addr}': {e}"))?;
        tracing::info!("SOCKS requested at {addr}");
        Some(listen)
    } else {
        None
    };
    if let Some(ref ns) = nameserver {
        tracing::info!("Nameserver requested at {ns} (runtime pending)");
    }

    let config = compat_cfg.clone().into_runtime_config();

    let signing_key = if !config.private_key.is_empty() {
        config
            .signing_key()
            .map_err(|e| format!("invalid private key: {e}"))?
    } else {
        SigningKey::generate(&mut rand::rngs::OsRng)
    };

    let public_key = signing_key.verifying_key().to_bytes();

    if m.opt_present("address") {
        println!("{}", addr_for_key(&public_key));
        return Ok(());
    }
    if m.opt_present("subnet") {
        println!("{}", subnet_for_key(&public_key));
        return Ok(());
    }
    if m.opt_present("publickey") {
        println!("{}", hex::encode(public_key));
        return Ok(());
    }

    let core = Core::new(signing_key, config.clone());
    tracing::info!("Your IPv6 address is {}", core.address());
    tracing::info!("Your IPv6 subnet is {}", core.subnet());
    tracing::info!("Your public key is {}", hex::encode(core.public_key()));

    core.init_links().await;
    core.start().await;

    let stack = SmolStack::new(core.clone());

    let admin = match AdminSocket::new(&config.admin_listen, core.clone()).await {
        Ok(admin) => Some(admin),
        Err(e) => {
            tracing::warn!("Failed to start admin socket: {}", e);
            None
        }
    };

    if let Some(endpoint) = probe_tcp {
        let (host, port_s) = endpoint
            .rsplit_once(':')
            .ok_or_else(|| format!("invalid --probe-tcp endpoint '{endpoint}'"))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let ip = host
            .parse::<std::net::Ipv6Addr>()
            .map_err(|e| format!("invalid --probe-tcp IPv6 host '{host}': {e}"))?;
        let port = port_s
            .parse::<u16>()
            .map_err(|e| format!("invalid --probe-tcp port '{port_s}': {e}"))?;
        tracing::info!("Running smoltcp probe to [{ip}]:{port}");
        match stack
            .tcp_probe_http(ip, port, host, std::time::Duration::from_secs(10))
            .await
        {
            Ok(resp) => {
                let preview: String = resp.chars().take(256).collect();
                tracing::info!("Probe succeeded, first bytes:\n{}", preview);
            }
            Err(e) => {
                tracing::error!("Probe failed: {e}");
            }
        }
    }

    for mapping in local_tcp_mappings {
        let st = stack.clone();
        tokio::spawn(async move {
            if let Err(e) = run_local_tcp_mapping(st, mapping).await {
                tracing::error!("{e}");
            }
        });
    }

    for mapping in remote_tcp_mappings {
        let st = stack.clone();
        tokio::spawn(async move {
            if let Err(e) = run_remote_tcp_mapping(st, mapping).await {
                tracing::warn!("{e}");
            }
        });
    }

    for mapping in local_udp_mappings {
        let st = stack.clone();
        tokio::spawn(async move {
            if let Err(e) = run_local_udp_mapping(st, mapping).await {
                tracing::warn!("{e}");
            }
        });
    }

    if !local_udp.is_empty() || !remote_udp.is_empty() || !remote_tcp.is_empty() {
        tracing::info!(
            "Runtime services enabled for SOCKS5, local-tcp, remote-tcp, local-udp and remote-udp"
        );
    }

    for mapping in remote_udp_mappings {
        let st = stack.clone();
        tokio::spawn(async move {
            if let Err(e) = run_remote_udp_mapping(st, mapping).await {
                tracing::warn!("{e}");
            }
        });
    }

    if let Some(listen) = socks_listen {
        let st = stack.clone();
        let ns = nameserver.clone();
        tokio::spawn(async move {
            if let Err(e) = run_socks_server(st, listen, ns).await {
                tracing::error!("SOCKS server stopped: {e}");
            }
        });
    }

    tracing::info!("yggstack core started");
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down...");

    if let Some(admin) = &admin {
        admin.close();
    }
    core.close().await.ok();

    Ok(())
}
