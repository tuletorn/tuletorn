//! Hardware spec JSON embedding (plan §5, §7.2).

use crate::metrics::HardwareSpec;
use std::path::{Path, PathBuf};

/// Directory layout of one benchmark run, matching plan §5.
///
/// ```text
/// results/run_<timestamp>/
/// ├── report.md
/// ├── hardware_spec.json
/// ├── raw_metrics.json
/// ├── results.csv
/// ├── flamegraphs/
/// └── plots/
/// ```
#[derive(Debug, Clone)]
pub struct RunDirectory {
    pub root: PathBuf,
}

impl RunDirectory {
    /// Create `results/run_<timestamp>/` and its subdirectories.
    pub fn create(base: impl AsRef<Path>, timestamp: &str) -> Result<Self, std::io::Error> {
        // Colons are legal on POSIX but not on Windows or in many archive
        // formats, so the ISO timestamp is flattened for the directory name.
        let safe = timestamp.replace([':', '-'], "").replace('Z', "");
        let root = base.as_ref().join(format!("run_{safe}"));
        std::fs::create_dir_all(root.join("flamegraphs"))?;
        std::fs::create_dir_all(root.join("plots"))?;
        Ok(Self { root })
    }

    pub fn report_md(&self) -> PathBuf {
        self.root.join("report.md")
    }
    pub fn hardware_json(&self) -> PathBuf {
        self.root.join("hardware_spec.json")
    }
    pub fn raw_metrics_json(&self) -> PathBuf {
        self.root.join("raw_metrics.json")
    }
    pub fn results_csv(&self) -> PathBuf {
        self.root.join("results.csv")
    }
    pub fn flamegraphs(&self) -> PathBuf {
        self.root.join("flamegraphs")
    }
    pub fn plots(&self) -> PathBuf {
        self.root.join("plots")
    }
}

/// Write `hardware_spec.json`.
pub fn write_hardware_spec(dir: &RunDirectory, hw: &HardwareSpec) -> Result<(), std::io::Error> {
    std::fs::write(dir.hardware_json(), hw.to_json())
}

/// Render the hardware spec as a Markdown section for the report.
#[must_use]
pub fn hardware_markdown(hw: &HardwareSpec) -> String {
    format!(
        "### Hardware & Environment\n\n\
         | Property | Value |\n| :--- | :--- |\n\
         | CPU | {} |\n\
         | Cores | {} physical / {} logical |\n\
         | CPU frequency | {} MHz |\n\
         | RAM | {:.2} GB |\n\
         | OS | {} |\n\
         | Kernel | {} |\n\
         | Architecture | {} |\n\
         | Vector features | {} |\n\
         | Rust toolchain | {} |\n\
         | Build profile | {} |\n\
         | Docker | {} |\n\
         | kind | {} |\n\
         | Captured | {} |\n\n",
        hw.cpu_model,
        hw.cpu_cores_physical,
        hw.cpu_cores_logical,
        hw.cpu_frequency_mhz,
        hw.ram_total_gb,
        hw.os,
        hw.kernel,
        hw.arch,
        hw.target_cpu,
        hw.rust_version,
        hw.build_profile,
        hw.docker_version,
        hw.kind_version,
        hw.timestamp_utc,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory unique to this test, so parallel tests cannot delete each
    /// other's fixtures.
    fn temp_base(test: &str) -> PathBuf {
        std::env::temp_dir().join(format!("lb-bench-report-{}-{test}", std::process::id()))
    }

    #[test]
    fn run_directory_creates_the_layout_from_the_plan() {
        let base = temp_base("layout");
        let dir = RunDirectory::create(&base, "2026-08-28T12:00:00Z").expect("create");
        assert!(dir.flamegraphs().is_dir());
        assert!(dir.plots().is_dir());
        assert!(
            dir.root
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("run_")
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn directory_name_has_no_characters_that_break_archives() {
        let base = temp_base("naming");
        let dir = RunDirectory::create(&base, "2026-08-28T12:34:56Z").expect("create");
        let name = dir.root.file_name().unwrap().to_string_lossy().into_owned();
        assert!(!name.contains(':'), "{name}");
        assert_eq!(name, "run_20260828T123456");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn hardware_json_is_written_and_parseable() {
        let base = temp_base("json");
        let dir = RunDirectory::create(&base, "2026-01-01T00:00:00Z").expect("create");
        let hw = HardwareSpec::detect();
        write_hardware_spec(&dir, &hw).expect("write");
        let text = std::fs::read_to_string(dir.hardware_json()).expect("read");
        let parsed: HardwareSpec = serde_json::from_str(&text).expect("round trip");
        assert_eq!(parsed.arch, hw.arch);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn markdown_reports_the_detected_toolchain_not_a_literal() {
        let hw = HardwareSpec::detect();
        let md = hardware_markdown(&hw);
        assert!(
            md.contains(&hw.rust_version),
            "must embed the probed rustc version"
        );
        assert!(md.contains(&hw.cpu_model));
        assert!(md.contains("Docker"));
        assert!(md.contains("kind"));
    }
}
