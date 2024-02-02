use super::{Error, Result};
use nix::sys::resource::Resource;
const EVALUATION_LICENSE_KEY: &str = "EVALUATION-WITH-TELEMETRY";
const DEBUG_NO_LIMIT_LICENSE_KEY: &str = "DEBUG-NO-LIMIT";

#[derive(Clone, Copy)]
pub struct ResourceLimit(pub Resource, pub u64);

#[derive(Clone, Copy)]
pub enum License {
    NoLicenseKey,
    Evaluation,
    DebugNoLimit,
}

impl License {
    pub fn new(license_key: Option<String>) -> Result<Self> {
        let license = if let Some(license_key) = license_key {
            match license_key.trim() {
                EVALUATION_LICENSE_KEY => License::Evaluation,
                DEBUG_NO_LIMIT_LICENSE_KEY => License::DebugNoLimit,
                "" => License::NoLicenseKey,
                _ => return Err(Error::InvalidLicenseKey),
            }
        } else {
            License::NoLicenseKey
        };
        Ok(license)
    }

    pub fn get_resource_limits(self) -> Vec<ResourceLimit> {
        match self {
            License::NoLicenseKey => {
                vec![ResourceLimit(Resource::RLIMIT_CPU, 3600)]
            }
            _ => vec![],
        }
    }
}
