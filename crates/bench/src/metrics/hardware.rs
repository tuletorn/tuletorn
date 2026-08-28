//! Hardware and toolchain spec collection (plan §7.2).
//!
//! Every field is probed at run time. The previous implementation reported the
//! *package* version (`0.1.0`) as `rust_version` and hardcoded the toolchain
//! string into the Markdown report, so a report could claim a toolchain the
//! binary had not been built with.

use serde::{Deserialize, Serialize};
use std::process::Command;
use sysinfo::System;

/// A full environment snapshot, embedded in every report for reproducibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareSpec {
    pub cpu_model: String,
    pub cpu_cores_physical: usize,
    pub cpu_cores_logical: usize,
    pub cpu_frequency_mhz: u64,
    pub ram_total_gb: f64,
    pub os: String,
    pub kernel: String,
    pub arch: String,
    pub rust_version: String,
    pub docker_version: String,
    pub kind_version: String,
    pub target_cpu: String,
    pub build_profile: String,
    pub timestamp_utc: String,
}

impl HardwareSpec {
    /// Probe the machine and toolchain.
    #[must_use]
    pub fn detect() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();

        let cpu_model = sys
            .cpus()
            .first()
            .map(|c| c.brand().trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "Unknown CPU".to_string());
        let cpu_frequency_mhz = sys.cpus().first().map(sysinfo::Cpu::frequency).unwrap_or(0);

        Self {
            cpu_model,
            cpu_cores_physical: System::physical_core_count()
                .unwrap_or_else(num_cpus::get_physical),
            cpu_cores_logical: sys.cpus().len().max(num_cpus::get()),
            cpu_frequency_mhz,
            ram_total_gb: sys.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0),
            os: format!(
                "{} {}",
                System::name().unwrap_or_else(|| "Unknown".into()),
                System::os_version().unwrap_or_else(|| "?".into())
            ),
            kernel: System::kernel_version().unwrap_or_else(|| "Unknown".into()),
            arch: std::env::consts::ARCH.to_string(),
            rust_version: tool_version("rustc", &["--version"]),
            docker_version: tool_version("docker", &["--version"]),
            kind_version: tool_version("kind", &["--version"]),
            target_cpu: detect_target_cpu(),
            build_profile: if cfg!(debug_assertions) {
                "debug".into()
            } else {
                "release".into()
            },
            timestamp_utc: iso8601_now(),
        }
    }

    /// Serialize to pretty JSON for `hardware_spec.json`.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Run `tool args...` and return its first output line, or `"not installed"`.
fn tool_version(tool: &str, args: &[&str]) -> String {
    Command::new(tool)
        .args(args)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| {
            String::from_utf8(out.stdout)
                .ok()
                .and_then(|s| s.lines().next().map(str::trim).map(str::to_owned))
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "not installed".to_string())
}

/// The `target-cpu` this binary was compiled for, as far as it can be known.
fn detect_target_cpu() -> String {
    // RUSTFLAGS is not visible at run time, so report what the build config
    // implies plus the architectural features actually compiled in.
    let mut features: Vec<&str> = Vec::new();
    #[cfg(target_arch = "x86_64")]
    {
        if cfg!(target_feature = "avx512f") {
            features.push("avx512f");
        }
        if cfg!(target_feature = "avx2") {
            features.push("avx2");
        }
        if cfg!(target_feature = "sse4.2") {
            features.push("sse4.2");
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if cfg!(target_feature = "neon") {
            features.push("neon");
        }
        if cfg!(target_feature = "sve") {
            features.push("sve");
        }
    }
    if features.is_empty() {
        std::env::consts::ARCH.to_string()
    } else {
        format!("{} ({})", std::env::consts::ARCH, features.join(", "))
    }
}

/// Format the current time as an ISO 8601 UTC timestamp.
///
/// Implemented directly from the Unix epoch rather than pulling in a date
/// library, using the civil-from-days algorithm (Howard Hinnant's `chrono`
/// proposal), which is exact for all dates after 1970.
fn iso8601_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_iso8601(secs)
}

fn format_iso8601(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (hour, minute, second) = (rem / 3_600, (rem % 3_600) / 60, rem % 60);

    // civil_from_days: shift the epoch to 0000-03-01 so leap days land last.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_a_plausible_machine() {
        let hw = HardwareSpec::detect();
        assert!(hw.cpu_cores_logical >= 1);
        assert!(hw.cpu_cores_physical >= 1);
        assert!(hw.ram_total_gb > 0.0);
        assert!(!hw.arch.is_empty());
        assert_eq!(
            hw.build_profile,
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            }
        );
    }

    #[test]
    fn rust_version_is_the_toolchain_not_the_package_version() {
        let hw = HardwareSpec::detect();
        assert_ne!(
            hw.rust_version,
            env!("CARGO_PKG_VERSION"),
            "must report rustc, not the crate version"
        );
        assert!(
            hw.rust_version.starts_with("rustc ") || hw.rust_version == "not installed",
            "unexpected rustc version string: {}",
            hw.rust_version
        );
    }

    #[test]
    fn missing_tools_are_reported_not_fatal() {
        assert_eq!(
            tool_version("definitely-not-a-real-binary", &["--version"]),
            "not installed"
        );
    }

    #[test]
    fn iso8601_matches_known_timestamps() {
        assert_eq!(format_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_iso8601(1_000_000_000), "2001-09-09T01:46:40Z");
        // 2024-02-29, a leap day.
        assert_eq!(format_iso8601(1_709_164_800), "2024-02-29T00:00:00Z");
        assert_eq!(format_iso8601(1_735_689_599), "2024-12-31T23:59:59Z");
    }

    #[test]
    fn serializes_to_json_with_every_plan_field() {
        let json = HardwareSpec::detect().to_json();
        for field in [
            "cpu_model",
            "cpu_cores_physical",
            "ram_total_gb",
            "os",
            "kernel",
            "docker_version",
            "rust_version",
            "kind_version",
        ] {
            assert!(json.contains(field), "hardware spec is missing {field}");
        }
    }
}
