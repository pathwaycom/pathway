use std::borrow::Borrow;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Write;
use std::fs;
use std::time::Duration;

use crate::engine::error::DynResult;
use base64::{prelude::BASE64_STANDARD, Engine};
use cached::proc_macro::cached;
use chrono::{DateTime, Utc};
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::{PUBLIC_KEY_LENGTH, SIGNATURE_LENGTH};
use hex::FromHex;
use log::{debug, warn};
use nix::sys::resource::Resource;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json;
use serde_json::json;

use super::error::DynError;

const PATHWAY_LICENSE_SERVER: &str = "https://license.pathway.com";

const PUBLIC_KEY: &str = "c6ef2abddc7da08f7c107649613a8f7dd853df3c54f6cafa656fc8b66fb3e8f3";
const LICENSE_ALGORITHM: &str = "base64+ed25519";

#[derive(Clone, Copy)]
pub struct ResourceLimit(pub Resource, pub u64);

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum License {
    LicenseKey(String),
    OfflineLicense(LicenseDetails),
    NoLicenseKey,
}

impl License {
    pub fn new(license_key: Option<String>) -> Result<Self, Error> {
        let license: Self = match read_license_to_string(license_key)? {
            key if key.is_empty() => Ok(Self::NoLicenseKey),
            key if key.starts_with("-----BEGIN LICENSE FILE-----") => {
                let license = read_license_content(key)?;
                Ok(Self::OfflineLicense(license))
            }
            key => Ok(Self::LicenseKey(key)),
        }?;
        if cfg!(feature = "enterprise") {
            license.check_required_policy("enterprise")?;
        } else if let Self::OfflineLicense(_) = license {
            return Err(Error::OfflineLicenseNotAllowed);
        }
        Ok(license)
    }

    pub fn check_entitlements(
        &self,
        entitlements: impl IntoIterator<Item = impl Borrow<str>>,
    ) -> Result<(), Error> {
        let entitlements: Vec<String> = entitlements
            .into_iter()
            .map(|s| s.borrow().to_uppercase())
            .collect();
        match self {
            License::NoLicenseKey => Err(Error::InsufficientLicenseEntitlements(entitlements)),
            License::OfflineLicense(license) => {
                let valid = entitlements
                    .iter()
                    .all(|e| license.entitlements.contains(e));
                if valid {
                    Ok(())
                } else {
                    Err(Error::InsufficientLicenseEntitlements(entitlements))
                }
            }
            License::LicenseKey(key) => {
                check_license_key_entitlements(key.clone(), entitlements)?;
                Ok(())
            }
        }
    }

    pub fn telemetry_required(&self) -> bool {
        match self {
            License::NoLicenseKey => false,
            License::OfflineLicense(license) => license.telemetry_required,
            License::LicenseKey(key) => check_license_key_entitlements(key.clone(), vec![])
                .map_or(false, |result| result.telemetry_required()),
        }
    }

    pub fn shortcut(&self) -> String {
        match self {
            License::LicenseKey(key) => {
                let pattern = r"^([^-\s]+-){4}[^-\s]+(-[^-\s]+)*$";
                let re = Regex::new(pattern).unwrap();

                if re.is_match(key) {
                    let parts: Vec<&str> = key.split('-').collect();
                    format!("{}-{}", parts[0], parts[1])
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        }
    }

    fn check_required_policy(&self, policy: &str) -> Result<(), Error> {
        match self {
            License::OfflineLicense(license) => {
                if license.policy == policy {
                    Ok(())
                } else {
                    Err(Error::InsufficientLicense)
                }
            }
            _ => Err(Error::InsufficientLicense),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("one of the features you used {0:?} requires upgrading your Pathway license.\nFor more information and to obtain your license key, visit: https://pathway.com/get-license/")]
    InsufficientLicenseEntitlements(Vec<String>),
    #[error("insufficient license.\nFor more information and to obtain your license key, visit: https://pathway.com/get-license/")]
    InsufficientLicense,
    #[error("offline license not allowed")]
    OfflineLicenseNotAllowed,
    #[error("unable to validate license: {0}")]
    LicenseValidationError(String),
    #[error("unable to validate license: malformed license")]
    MalformedLicense,
}

#[derive(Deserialize, Clone)]
struct ValidationResponse {
    valid: bool,
    details: HashMap<String, serde_json::Value>,
}

impl ValidationResponse {
    pub fn telemetry_required(&self) -> bool {
        let key = "telemetry_required".to_string();
        if self.details.contains_key(&key) {
            let telemetry = self.details.get(&key).unwrap();
            if let serde_json::Value::Bool(telemetry_required) = telemetry {
                return *telemetry_required;
            }
        }
        true
    }
}

#[cached]
fn check_license_key_entitlements(
    license_key: String,
    entitlements: Vec<String>,
) -> Result<ValidationResponse, Error> {
    KeygenLicenseChecker::new(PATHWAY_LICENSE_SERVER.to_string())
        .check_entitlements(&license_key, entitlements)
}

struct KeygenLicenseChecker {
    api_url: String,
    client: reqwest::blocking::Client,
}

impl KeygenLicenseChecker {
    fn new(api_url: String) -> Self {
        let client: reqwest::blocking::Client = reqwest::blocking::Client::builder()
            .build()
            .expect("initializing license checker should not fail");
        Self { api_url, client }
    }

    fn check_entitlements(
        &self,
        license_key: &str,
        entitlements: Vec<String>,
    ) -> Result<ValidationResponse, Error> {
        let response = self
            .client
            .post(format!("{}/license/validate", self.api_url))
            .header("Api-Version", "v1")
            .timeout(Duration::from_secs(10))
            .json(&json!({
                "license_key": license_key,
                "entitlements": entitlements
            }))
            .send()
            .map_err(|e| {
                let mut source = e.source();
                let mut report = format!("{e}");
                while let Some(source_inner) = source {
                    let _ = write!(report, "\nSource: {source_inner}");
                    source = source_inner.source();
                }
                Error::LicenseValidationError(report)
            })?;

        if response.status().is_success() {
            let result = response
                .json::<ValidationResponse>()
                .map_err(|_| Error::LicenseValidationError("malformed response".to_string()))?;
            if result.valid {
                Ok(result)
            } else {
                Err(Error::InsufficientLicenseEntitlements(entitlements))
            }
        } else {
            let status = response.status();
            Err(Error::LicenseValidationError(
                status.canonical_reason().unwrap_or("Unknown error").into(),
            ))
        }
    }
}

#[derive(Deserialize, Debug)]
struct LicenseFile<'a> {
    enc: &'a str,
    sig: &'a str,
    alg: &'a str,
}

#[derive(Serialize, Debug, Clone, Hash, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct LicenseDetails {
    telemetry_required: bool,
    entitlements: Vec<String>,
    policy: String,
    expiration_date: Option<DateTime<Utc>>,
}

fn deserialize_signed_license(public_key: &str, license: &str) -> DynResult<serde_json::Value> {
    let public_key = VerifyingKey::from_bytes(&<[u8; PUBLIC_KEY_LENGTH]>::from_hex(public_key)?)?;

    // load license
    let cleaned_license = license
        .replace("-----BEGIN LICENSE FILE-----", "")
        .replace("-----END LICENSE FILE-----", "")
        .replace('\n', "");

    let license_data = String::from_utf8(BASE64_STANDARD.decode(cleaned_license)?)?;
    let license_file: LicenseFile = serde_json::from_str(&license_data)?;

    // assert license crypto algorithm
    if license_file.alg != LICENSE_ALGORITHM {
        return Err(DynError::from(format!(
            "algorithm {:?} not supported",
            license_file.alg,
        )));
    }

    // verify signature
    let message = format!("license/{}", license_file.enc);
    let signature: [u8; SIGNATURE_LENGTH] = BASE64_STANDARD
        .decode(license_file.sig)?
        .try_into()
        .map_err(|_| "signature format is invalid")?;
    public_key
        .verify_strict(message.as_bytes(), &signature.into())
        .map_err(|_| "license file is invalid")?;

    let payload = String::from_utf8(BASE64_STANDARD.decode(license_file.enc)?)?;
    let license_content: serde_json::Value = serde_json::from_str(&payload)?;

    Ok(license_content)
}

#[cached]
fn read_license_content(license: String) -> Result<LicenseDetails, Error> {
    let lic = deserialize_signed_license(PUBLIC_KEY, &license.clone())
        .map_err(|e| Error::LicenseValidationError(e.to_string()))?;

    let included = lic["included"].as_array().ok_or(Error::MalformedLicense)?;

    let mut entitlements: Vec<String> = vec![];
    let mut policy: Option<String> = None;
    let mut telemetry_required: bool = false;

    for item in included {
        match item["type"].as_str() {
            Some("entitlements") => {
                let item_code = item["attributes"]["code"]
                    .as_str()
                    .ok_or(Error::MalformedLicense)?;
                entitlements.push(item_code.to_string());
            }
            Some("policies") => {
                policy = Some(
                    item["attributes"]["name"]
                        .as_str()
                        .ok_or(Error::MalformedLicense)?
                        .to_string(),
                );
                telemetry_required = item["attributes"]["metadata"]["telemetryRequired"]
                    .as_bool()
                    .unwrap_or(false);
            }
            _ => (),
        }
    }

    let policy = policy.ok_or(Error::MalformedLicense)?;

    let parse_datetime = |value: &serde_json::Value| -> Result<Option<DateTime<Utc>>, Error> {
        match value.as_str() {
            Some(d) => d
                .parse::<DateTime<Utc>>()
                .map(Some)
                .map_err(|_| Error::MalformedLicense),
            None => Ok(None),
        }
    };

    let expiration_date = parse_datetime(&lic["data"]["attributes"]["expiry"])?;
    let license_file_ttl = parse_datetime(&lic["meta"]["expiry"])?;

    let is_expired = |date: Option<DateTime<Utc>>| date.is_some_and(|d| d < Utc::now());

    if is_expired(expiration_date) {
        warn!("License has expired. Please renew to continue using the service.");
    } else if is_expired(license_file_ttl) {
        warn!("License file's time-to-live has been exceeded. Please update the license file.");
    }

    let result = LicenseDetails {
        telemetry_required,
        entitlements,
        policy,
        expiration_date,
    };

    debug!(
        "License > {}",
        serde_json::to_string_pretty(&result).unwrap()
    );

    Ok(result)
}

#[cached]
pub fn read_license_to_string(license_key_or_path: Option<String>) -> Result<String, Error> {
    if let Some(key_or_path) = license_key_or_path {
        let key_or_path = key_or_path.trim();
        if let Some(file_path) = key_or_path.strip_prefix("file://") {
            fs::read_to_string(file_path).map_err(|e| Error::LicenseValidationError(e.to_string()))
        } else {
            Ok(key_or_path.to_string())
        }
    } else {
        Ok(String::new())
    }
}
