use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};

use yggdrasil::config::Config;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CompatConfig {
    #[serde(rename = "PrivateKey")]
    pub private_key: String,
    #[serde(rename = "Peers")]
    pub peers: Vec<String>,
    #[serde(rename = "Listen")]
    pub listen: Vec<String>,
    #[serde(rename = "AdminListen")]
    pub admin_listen: String,
    #[serde(rename = "AllowedPublicKeys")]
    pub allowed_public_keys: Vec<String>,
    #[serde(rename = "NodeInfoPrivacy")]
    pub node_info_privacy: bool,
    #[serde(rename = "NodeInfo")]
    pub node_info: serde_json::Value,
    /// Closed-network group password (yggdrasil-ng extension, not in Go yggstack).
    /// When non-empty, sessions only complete with nodes sharing the same password.
    #[serde(rename = "GroupPassword")]
    pub group_password: String,
}

impl Default for CompatConfig {
    fn default() -> Self {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        Self {
            private_key: hex::encode(signing_key.to_keypair_bytes()),
            peers: vec![],
            listen: vec!["tcp://0.0.0.0:0".to_string()],
            admin_listen: "none".to_string(),
            allowed_public_keys: vec![],
            node_info_privacy: false,
            node_info: serde_json::json!({}),
            group_password: String::new(),
        }
    }
}

impl CompatConfig {
    pub fn into_runtime_config(self) -> Config {
        Config {
            private_key: self.private_key,
            peers: self.peers,
            listen: self.listen,
            admin_listen: self.admin_listen,
            if_name: "none".to_string(),
            if_mtu: 65535,
            if_dns_servers: Vec::new(),
            tunnel_routing: Default::default(),
            node_info: json_to_toml(&self.node_info),
            node_info_privacy: self.node_info_privacy,
            allowed_public_keys: self.allowed_public_keys,
            multicast_interfaces: vec![],
            firewall: Default::default(),
            group_password: self.group_password,
        }
    }
}

pub fn parse_compat_config(text: &str) -> Result<CompatConfig, String> {
    match serde_json::from_str::<CompatConfig>(text) {
        Ok(cfg) => Ok(cfg),
        Err(json_err) => match json5::from_str::<CompatConfig>(text) {
            Ok(cfg) => Ok(cfg),
            Err(json5_err) => Err(format!(
                "failed to parse config as JSON ({json_err}) or relaxed JSON/HJSON-style syntax ({json5_err})"
            )),
        },
    }
}

fn json_to_toml(v: &serde_json::Value) -> toml::Value {
    match v {
        serde_json::Value::Null => toml::Value::Table(toml::map::Map::new()),
        serde_json::Value::Bool(b) => toml::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                toml::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                toml::Value::Float(f)
            } else {
                toml::Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => toml::Value::String(s.clone()),
        serde_json::Value::Array(a) => toml::Value::Array(a.iter().map(json_to_toml).collect()),
        serde_json::Value::Object(o) => {
            let mut m = toml::map::Map::new();
            for (k, vv) in o {
                m.insert(k.clone(), json_to_toml(vv));
            }
            toml::Value::Table(m)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_config() {
        let text = r#"{
            "PrivateKey": "abcd",
            "Peers": ["tcp://127.0.0.1:1234"],
            "NodeInfo": {"name":"node-a"}
        }"#;
        let cfg = parse_compat_config(text).expect("parse config");
        assert_eq!(cfg.private_key, "abcd");
        assert_eq!(cfg.peers.len(), 1);
    }

    #[test]
    fn parse_relaxed_json5_style_config() {
        let text = r#"{
            // comment
            PrivateKey: "abcd",
            Peers: ["tcp://127.0.0.1:1234"],
            NodeInfo: {name: "node-a"},
        }"#;
        let cfg = parse_compat_config(text).expect("parse config");
        assert_eq!(cfg.private_key, "abcd");
        assert_eq!(cfg.peers.len(), 1);
        assert_eq!(cfg.node_info["name"], "node-a");
    }

    #[test]
    fn default_config_has_valid_key() {
        let cfg = CompatConfig::default();
        // Ed25519 keypair is 64 bytes = 128 hex chars
        assert_eq!(cfg.private_key.len(), 128);
        assert!(hex::decode(&cfg.private_key).is_ok());
    }

    #[test]
    fn default_config_into_runtime() {
        let cfg = CompatConfig::default();
        let key = cfg.private_key.clone();
        let rt = cfg.into_runtime_config();
        assert_eq!(rt.private_key, key);
        assert_eq!(rt.if_mtu, 65535);
        assert_eq!(rt.if_name, "none");
    }

    #[test]
    fn parse_empty_config_uses_defaults() {
        let cfg = parse_compat_config("{}").expect("parse empty");
        assert_eq!(cfg.peers.len(), 0);
        assert_eq!(cfg.admin_listen, "none");
        assert_eq!(cfg.node_info_privacy, false);
    }

    #[test]
    fn group_password_passes_through() {
        let cfg = parse_compat_config(r#"{"GroupPassword": "s3cr3t"}"#).expect("parse");
        assert_eq!(cfg.group_password, "s3cr3t");
        let rt = cfg.into_runtime_config();
        assert_eq!(rt.group_password, "s3cr3t");
        // Absent field defaults to empty (open network)
        let rt = parse_compat_config("{}").unwrap().into_runtime_config();
        assert_eq!(rt.group_password, "");
    }

    #[test]
    fn parse_invalid_json_fails() {
        assert!(parse_compat_config("not json at all").is_err());
    }

    #[test]
    fn json_to_toml_roundtrip() {
        let json = serde_json::json!({
            "flag": true,
            "count": 42,
            "pi": 3.14,
            "name": "test",
            "items": [1, 2, 3],
            "nested": {"inner": "value"},
            "empty": null
        });
        let toml_val = json_to_toml(&json);
        match toml_val {
            toml::Value::Table(t) => {
                assert_eq!(t["flag"], toml::Value::Boolean(true));
                assert_eq!(t["count"], toml::Value::Integer(42));
                assert_eq!(t["name"], toml::Value::String("test".into()));
                assert!(matches!(t["pi"], toml::Value::Float(_)));
                assert!(matches!(t["items"], toml::Value::Array(_)));
                assert!(matches!(t["nested"], toml::Value::Table(_)));
                assert!(matches!(t["empty"], toml::Value::Table(_))); // null → empty table
            }
            _ => panic!("expected table"),
        }
    }
}
