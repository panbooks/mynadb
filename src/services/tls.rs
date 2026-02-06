//! TLS/SSL configuration and certificate loading for all server protocols
//!
//! Provides TLS config builders for:
//! - HTTP (axum-server with rustls)
//! - Arrow Flight gRPC (tonic with rustls)
//! - MySQL (msql-srv with rustls 0.20)

use crate::config::TlsConfig;
use std::fs;
use std::io::BufReader;
use std::sync::Arc;
use tracing::info;

/// Load PEM certificate chain and private key from files
fn load_pem_files(config: &TlsConfig) -> Result<(Vec<Vec<u8>>, Vec<u8>), String> {
    // Read certificate chain
    let cert_data =
        fs::read(&config.cert_file).map_err(|e| format!("Failed to read cert_file: {}", e))?;
    let mut cert_reader = BufReader::new(cert_data.as_slice());
    let certs: Vec<Vec<u8>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse certificates: {}", e))?
        .into_iter()
        .map(|c| c.to_vec())
        .collect();

    if certs.is_empty() {
        return Err("No certificates found in cert_file".to_string());
    }

    // Read private key
    let key_data =
        fs::read(&config.key_file).map_err(|e| format!("Failed to read key_file: {}", e))?;
    let mut key_reader = BufReader::new(key_data.as_slice());

    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| format!("Failed to parse private key: {}", e))?
        .ok_or_else(|| "No private key found in key_file".to_string())?
        .secret_der()
        .to_vec();

    info!(
        "Loaded TLS certificates: {} cert(s) from {}",
        certs.len(),
        config.cert_file
    );

    Ok((certs, key))
}

/// Build axum-server RustlsConfig for HTTP TLS
pub async fn axum_tls_config(
    config: &TlsConfig,
) -> Result<axum_server::tls_rustls::RustlsConfig, String> {
    config.validate()?;

    let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
        &config.cert_file,
        &config.key_file,
    )
    .await
    .map_err(|e| format!("Failed to build HTTP TLS config: {}", e))?;

    info!("HTTP TLS configuration loaded");
    Ok(rustls_config)
}

/// Build tonic ServerTlsConfig for Arrow Flight gRPC TLS
pub fn tonic_tls_config(config: &TlsConfig) -> Result<tonic::transport::ServerTlsConfig, String> {
    config.validate()?;

    let cert_pem =
        fs::read_to_string(&config.cert_file).map_err(|e| format!("Failed to read cert: {}", e))?;
    let key_pem =
        fs::read_to_string(&config.key_file).map_err(|e| format!("Failed to read key: {}", e))?;

    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);

    // Optional mTLS with CA certificate
    if let Some(ref ca_file) = config.ca_file {
        if !ca_file.is_empty() {
            let ca_pem =
                fs::read_to_string(ca_file).map_err(|e| format!("Failed to read CA cert: {}", e))?;
            let ca_cert = tonic::transport::Certificate::from_pem(ca_pem);
            tls = tls.client_ca_root(ca_cert);
            info!("Flight TLS: mTLS enabled with CA from {}", ca_file);
        }
    }

    info!("Flight TLS configuration loaded");
    Ok(tls)
}

/// Build rustls 0.20 ServerConfig for MySQL TLS (msql-srv compatibility)
///
/// msql-srv 0.10 uses rustls 0.20.x which has a different API than modern rustls.
/// The `MysqlShim::tls_config()` trait method expects `Arc<rustls::ServerConfig>`
/// where `rustls` is version 0.20.
pub fn mysql_tls_config(config: &TlsConfig) -> Result<Arc<rustls::ServerConfig>, String> {
    config.validate()?;

    let (certs, key) = load_pem_files(config)?;

    // Convert to rustls 0.20 types
    let certs: Vec<rustls::Certificate> = certs.into_iter().map(rustls::Certificate).collect();
    let key = rustls::PrivateKey(key);

    let mut server_config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("Failed to build MySQL TLS config: {}", e))?;

    // Enable TLS 1.2 and 1.3 (MySQL clients commonly use TLS 1.2)
    server_config.alpn_protocols = vec![];

    info!("MySQL TLS configuration loaded");
    Ok(Arc::new(server_config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_test_tls_config(dir: &tempfile::TempDir) -> TlsConfig {
        // Generate a self-signed certificate for testing using rcgen or just write test PEM files
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        // Minimal self-signed cert/key for testing (generated with openssl)
        // These are intentionally dummy values that parse correctly as PEM
        // but won't pass real TLS handshakes
        let cert_pem = include_str!("../../tests/fixtures/test_cert.pem");
        let key_pem = include_str!("../../tests/fixtures/test_key.pem");

        fs::write(&cert_path, cert_pem).unwrap();
        fs::write(&key_path, key_pem).unwrap();

        TlsConfig {
            enabled: true,
            cert_file: cert_path.to_string_lossy().to_string(),
            key_file: key_path.to_string_lossy().to_string(),
            ca_file: None,
        }
    }

    #[test]
    fn test_tls_config_validation_disabled() {
        let config = TlsConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_config_validation_missing_cert() {
        let config = TlsConfig {
            enabled: true,
            cert_file: String::new(),
            key_file: String::new(),
            ca_file: None,
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("cert_file"));
    }

    #[test]
    fn test_tls_config_validation_missing_key() {
        let dir = tempfile::TempDir::new().unwrap();
        let cert_path = dir.path().join("cert.pem");
        fs::write(&cert_path, "dummy").unwrap();

        let config = TlsConfig {
            enabled: true,
            cert_file: cert_path.to_string_lossy().to_string(),
            key_file: String::new(),
            ca_file: None,
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("key_file"));
    }

    #[test]
    fn test_tls_config_validation_file_not_found() {
        let config = TlsConfig {
            enabled: true,
            cert_file: "/nonexistent/cert.pem".to_string(),
            key_file: "/nonexistent/key.pem".to_string(),
            ca_file: None,
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("not found"));
    }
}
