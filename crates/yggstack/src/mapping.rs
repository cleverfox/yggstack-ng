use std::net::IpAddr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointSpec {
    pub host: Option<String>,
    pub port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappingSpec {
    pub first: EndpointSpec,
    pub second: EndpointSpec,
}

fn parse_port(s: &str, raw: &str) -> Result<u16, String> {
    let p: u16 = s
        .parse()
        .map_err(|_| format!("Malformed mapping spec '{raw}'"))?;
    if p == 0 {
        return Err("Ports must not be zero".to_string());
    }
    Ok(p)
}

fn parse_mapping_string(raw: &str) -> Result<(Option<String>, u16, Option<String>, u16), String> {
    let mut first_addr: Option<String> = None;
    let mut second_addr: Option<String> = None;

    let mut tokens: Vec<&str> = raw.split(':').collect();

    match tokens.len() {
        1 => {
            let first_port = parse_port(tokens[0], raw)?;
            return Ok((first_addr, first_port, second_addr, first_port));
        }
        2 => {
            let first_port = parse_port(tokens[0], raw)?;
            let second_port = parse_port(tokens[1], raw)?;
            return Ok((first_addr, first_port, second_addr, second_port));
        }
        3 => {
            let first_port = parse_port(tokens[0], raw)?;
            let host_port = format!("{}:{}", tokens[1], tokens[2]);
            let (h, p) =
                split_host_port(&host_port).map_err(|_| format!("Malformed mapping spec '{raw}'"))?;
            let second_port = parse_port(&p, raw)?;
            second_addr = Some(h);
            return Ok((first_addr, first_port, second_addr, second_port));
        }
        4 => {
            let hp1 = format!("{}:{}", tokens[0], tokens[1]);
            let hp2 = format!("{}:{}", tokens[2], tokens[3]);
            let (h1, p1) =
                split_host_port(&hp1).map_err(|_| format!("Malformed mapping spec '{raw}'"))?;
            let (h2, p2) =
                split_host_port(&hp2).map_err(|_| format!("Malformed mapping spec '{raw}'"))?;
            let first_port = parse_port(&p1, raw)?;
            let second_port = parse_port(&p2, raw)?;
            first_addr = Some(h1);
            second_addr = Some(h2);
            return Ok((first_addr, first_port, second_addr, second_port));
        }
        _ => {}
    }

    // Flexible form with IPv6 literals and optional first address:
    // [first-addr:]first-port:second-addr:second-port
    let second_port = parse_port(
        tokens
            .last()
            .ok_or_else(|| format!("Malformed mapping spec '{raw}'"))?,
        raw,
    )?;
    tokens.pop();

    if tokens.is_empty() {
        return Err(format!("Malformed mapping spec '{raw}'"));
    }

    if tokens.last().is_some_and(|s| s.ends_with(']')) {
        let mut i = tokens.len();
        while i > 0 {
            i -= 1;
            if tokens[i].starts_with('[') {
                let joined = tokens[i..].join(":");
                second_addr = Some(trim_brackets(&joined).to_string());
                tokens.truncate(i);
                break;
            }
        }
        if second_addr.is_none() {
            return Err(format!("Malformed mapping spec '{raw}'"));
        }
    } else {
        second_addr = Some(
            tokens
                .pop()
                .ok_or_else(|| format!("Malformed mapping spec '{raw}'"))?
                .to_string(),
        );
    }

    if tokens.is_empty() {
        return Err(format!("Malformed mapping spec '{raw}'"));
    }

    let first_port = parse_port(
        tokens
            .pop()
            .ok_or_else(|| format!("Malformed mapping spec '{raw}'"))?,
        raw,
    )?;

    if !tokens.is_empty() {
        if tokens.last().is_some_and(|s| s.ends_with(']')) {
            let mut i = tokens.len();
            while i > 0 {
                i -= 1;
                if tokens[i].starts_with('[') {
                    let joined = tokens[i..].join(":");
                    first_addr = Some(trim_brackets(&joined).to_string());
                    break;
                }
            }
        } else {
            first_addr = Some(
                tokens
                    .last()
                    .ok_or_else(|| format!("Malformed mapping spec '{raw}'"))?
                    .to_string(),
            );
        }
    }

    Ok((first_addr, first_port, second_addr, second_port))
}

fn trim_brackets(s: &str) -> &str {
    s.strip_prefix('[')
        .and_then(|v| v.strip_suffix(']'))
        .unwrap_or(s)
}

fn split_host_port(input: &str) -> Result<(String, String), ()> {
    match input.parse::<std::net::SocketAddr>() {
        Ok(sa) => Ok((sa.ip().to_string(), sa.port().to_string())),
        Err(_) => {
            // Fallback for hostnames that are not parseable as SocketAddr.
            if let Some((host, port)) = input.rsplit_once(':') {
                if host.is_empty() || port.is_empty() {
                    return Err(());
                }
                Ok((trim_brackets(host).to_string(), port.to_string()))
            } else {
                Err(())
            }
        }
    }
}

pub fn parse_local_mapping(raw: &str) -> Result<MappingSpec, String> {
    let (first_addr, first_port, second_addr, second_port) = parse_mapping_string(raw)?;

    let second_host =
        second_addr.ok_or_else(|| "Yggdrasil listening address can be only IPv6".to_string())?;
    let parsed = second_host
        .parse::<IpAddr>()
        .map_err(|_| format!("invalid mapped address '{second_host}'"))?;
    if !matches!(parsed, IpAddr::V6(_)) {
        return Err("Yggdrasil listening address can be only IPv6".to_string());
    }

    if let Some(ref host) = first_addr {
        host.parse::<IpAddr>()
            .map_err(|_| format!("invalid listen address '{host}'"))?;
    }

    Ok(MappingSpec {
        first: EndpointSpec {
            host: first_addr,
            port: first_port,
        },
        second: EndpointSpec {
            host: Some(second_host),
            port: second_port,
        },
    })
}

pub fn parse_remote_mapping(raw: &str) -> Result<MappingSpec, String> {
    let (first_addr, first_port, second_addr, second_port) = parse_mapping_string(raw)?;

    if first_addr.is_some() {
        return Err("Yggdrasil listening must be empty".to_string());
    }

    if let Some(ref host) = second_addr {
        host.parse::<IpAddr>()
            .map_err(|_| format!("invalid mapped address '{host}'"))?;
    }

    Ok(MappingSpec {
        first: EndpointSpec {
            host: None,
            port: first_port,
        },
        second: EndpointSpec {
            host: second_addr.or_else(|| Some("::1".to_string())),
            port: second_port,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_with_listen_host() {
        let m = parse_local_mapping("127.0.0.1:8080:[200:1::1]:80").expect("mapping parse");
        assert_eq!(m.first.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(m.first.port, 8080);
        assert_eq!(m.second.port, 80);
    }

    #[test]
    fn local_reject_non_v6_remote() {
        assert!(parse_local_mapping("8080:127.0.0.1:80").is_err());
    }

    #[test]
    fn remote_accept_simple() {
        let m = parse_remote_mapping("80:8080").expect("mapping parse");
        assert_eq!(m.first.port, 80);
        assert_eq!(m.second.host.as_deref(), Some("::1"));
        assert_eq!(m.second.port, 8080);
    }

    #[test]
    fn remote_reject_first_host() {
        assert!(parse_remote_mapping("127.0.0.1:80:127.0.0.1:8080").is_err());
    }

    #[test]
    fn local_simple_port_only() {
        let m = parse_local_mapping("8080:[200:1::1]:80").expect("parse");
        assert_eq!(m.first.host, None);
        assert_eq!(m.first.port, 8080);
        assert_eq!(m.second.host.as_deref(), Some("200:1::1"));
        assert_eq!(m.second.port, 80);
    }

    #[test]
    fn local_ipv6_bracket_full() {
        let m = parse_local_mapping("[::1]:8080:[200:abcd::1]:443").expect("parse");
        assert_eq!(m.first.host.as_deref(), Some("::1"));
        assert_eq!(m.first.port, 8080);
        assert_eq!(m.second.host.as_deref(), Some("200:abcd::1"));
        assert_eq!(m.second.port, 443);
    }

    #[test]
    fn local_same_port() {
        let m = parse_local_mapping("80:[200:1::1]:80").expect("parse");
        assert_eq!(m.first.port, 80);
        assert_eq!(m.second.port, 80);
    }

    #[test]
    fn remote_with_target_host() {
        let m = parse_remote_mapping("80:127.0.0.1:8080").expect("parse");
        assert_eq!(m.first.port, 80);
        assert_eq!(m.second.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(m.second.port, 8080);
    }

    #[test]
    fn remote_single_port() {
        let m = parse_remote_mapping("80").expect("parse");
        assert_eq!(m.first.port, 80);
        assert_eq!(m.second.port, 80);
        assert_eq!(m.second.host.as_deref(), Some("::1"));
    }

    #[test]
    fn reject_zero_port_local() {
        assert!(parse_local_mapping("0:[200:1::1]:80").is_err());
    }

    #[test]
    fn reject_zero_port_remote() {
        assert!(parse_remote_mapping("0:8080").is_err());
    }

    #[test]
    fn reject_empty() {
        assert!(parse_local_mapping("").is_err());
        assert!(parse_remote_mapping("").is_err());
    }

    #[test]
    fn reject_port_overflow() {
        assert!(parse_local_mapping("99999:[200:1::1]:80").is_err());
        assert!(parse_remote_mapping("99999:80").is_err());
    }
}
