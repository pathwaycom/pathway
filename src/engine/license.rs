use std::collections::HashMap;
use std::time::Duration;

use cached::proc_macro::cached;
use log::info;
use nix::sys::resource::Resource;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};

const PATHWAY_LICENSE_SERVER: &str = "https://license.pathway.com";

#[derive(Clone, Copy)]
pub struct ResourceLimit(pub Resource, pub u64);

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum License {
    LicenseKey(String),
    NoLicenseKey,
}

impl License {
    pub fn new(license_key: Option<String>) -> Self {
        if let Some(license_key) = license_key {
            match license_key.trim() {
                "" => License::NoLicenseKey,
                _ => License::LicenseKey(license_key),
            }
        } else {
            License::NoLicenseKey
        }
    }

    pub fn check_entitlements(
        &self,
        entitlements: Vec<String>,
    ) -> Result<ValidationResponse, Error> {
        check_entitlements(self.clone(), entitlements)
    }

    pub fn telemetry_required(&self) -> bool {
        match self {
            License::NoLicenseKey => false,
            License::LicenseKey(_) => {
                if let Ok(result) = self.check_entitlements(vec![]) {
                    result.telemetry_required()
                } else {
                    false
                }
            }
        }
    }

    pub fn shortcut(&self) -> String {
        match self {
            License::NoLicenseKey => String::new(),
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
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("one of the features you used {0:?} requires upgrading your Pathway license.\nFor more information and to obtain your license key, visit: https://pathway.com/get-license/")]
    InsufficientLicense(Vec<String>),
    #[error("unable to validate license")]
    LicenseValidationError,
}

#[derive(Deserialize, Clone)]
pub struct ValidationResponse {
    valid: bool,
    details: HashMap<String, Value>,
}

impl ValidationResponse {
    pub fn telemetry_required(&self) -> bool {
        let key = "telemetry_required".to_string();
        if self.details.contains_key(&key) {
            let telemetry = self.details.get(&key).unwrap();
            if let Value::Bool(telemetry_required) = telemetry {
                return *telemetry_required;
            }
        }
        true
    }
}

#[cached]
pub fn check_entitlements(
    license: License,
    entitlements: Vec<String>,
) -> Result<ValidationResponse, Error> {
    KeygenLicenseChecker::new(PATHWAY_LICENSE_SERVER.to_string())
        .check_entitlements(license, entitlements)
}

trait LicenseChecker {
    fn check_entitlements(
        &self,
        license: License,
        entitlements: Vec<String>,
    ) -> Result<ValidationResponse, Error>;
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
}

impl LicenseChecker for KeygenLicenseChecker {
    fn check_entitlements(
        &self,
        license: License,
        entitlements: Vec<String>,
    ) -> Result<ValidationResponse, Error> {
        info!("Checking entitlements: {:?}", entitlements);
        match license {
            License::LicenseKey(license_key) => {
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
                    .map_err(|_| Error::LicenseValidationError)?;

                if response.status().is_success() {
                    let result = response
                        .json::<ValidationResponse>()
                        .map_err(|_| Error::LicenseValidationError)?;
                    if result.valid {
                        Ok(result)
                    } else {
                        Err(Error::InsufficientLicense(entitlements))
                    }
                } else {
                    Err(Error::LicenseValidationError)
                }
            }
            License::NoLicenseKey => Err(Error::InsufficientLicense(entitlements)),
        }
    }
}
