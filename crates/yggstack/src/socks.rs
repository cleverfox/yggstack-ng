#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocksListen {
    Tcp(String),
    #[cfg(unix)]
    Unix(String),
}

pub fn parse_socks_listen(input: &str) -> Result<SocksListen, String> {
    if input.trim().is_empty() {
        return Err("socks listen address must not be empty".to_string());
    }

    if input.contains(':') {
        return Ok(SocksListen::Tcp(input.to_string()));
    }

    #[cfg(unix)]
    {
        Ok(SocksListen::Unix(input.to_string()))
    }

    #[cfg(not(unix))]
    {
        Err("unix socket SOCKS listeners are not supported on this platform".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tcp_listen() {
        let s = parse_socks_listen("127.0.0.1:1080").expect("parse socks");
        assert_eq!(s, SocksListen::Tcp("127.0.0.1:1080".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn parse_unix_listen() {
        let s = parse_socks_listen("/tmp/yggstack.sock").expect("parse socks");
        assert_eq!(s, SocksListen::Unix("/tmp/yggstack.sock".to_string()));
    }

    #[test]
    fn reject_empty_string() {
        assert!(parse_socks_listen("").is_err());
    }

    #[test]
    fn reject_whitespace_only() {
        assert!(parse_socks_listen("   ").is_err());
    }

    #[test]
    fn tcp_with_ipv6() {
        let s = parse_socks_listen("[::1]:1080").expect("parse socks");
        assert_eq!(s, SocksListen::Tcp("[::1]:1080".to_string()));
    }
}
