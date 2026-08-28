//! Markdown comparison report with embedded flamegraph links.

use crate::harness::pgo_improvement_pct;
use crate::metrics::{BenchmarkResult, HardwareSpec};
use crate::report::hardware_report::hardware_markdown;
use std::fmt::Write;
use std::path::Path;

/// Render the full comparison report.
#[must_use]
pub fn generate_markdown_report(hw: &HardwareSpec, results: &[BenchmarkResult]) -> String {
    let mut md = String::with_capacity(4096 + results.len() * 256);
    md.push_str("# Reverse Proxy Benchmark Report\n\n");
    md.push_str(&hardware_markdown(hw));

    if results.is_empty() {
        md.push_str("_No measurements were collected._\n");
        return md;
    }

    md.push_str(&summary_table(results));
    md.push_str(&tail_latency_table(results));
    md.push_str(&resource_table(results));
    md.push_str(&integrity_section(results));
    md
}

/// Headline throughput and latency, one row per measurement.
fn summary_table(results: &[BenchmarkResult]) -> String {
    let mut md = String::from("## Throughput & Latency\n\n");
    md.push_str(
        "| Candidate | Proto | Payload | Conn | RPS | Throughput | p50 | p99 | p99.9 |\n\
         | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for r in results {
        let _ = writeln!(
            md,
            "| **{}** | {} | {} | {} | **{:.0}** | {:.1} MiB/s | {} | {} | {} |",
            r.candidate_name,
            r.http_version,
            human_bytes(r.payload_bytes),
            r.concurrency,
            r.rps,
            r.throughput_mib_s,
            micros(r.latency_p50_us),
            micros(r.latency_p99_us),
            micros(r.latency_p999_us),
        );
    }
    md.push('\n');
    md
}

/// Raw vs coordinated-omission-corrected tail, which is the number that matters
/// for a closed-loop generator under saturation.
fn tail_latency_table(results: &[BenchmarkResult]) -> String {
    let mut md = String::from("## Tail Latency (raw vs. coordinated-omission corrected)\n\n");
    md.push_str(
        "A closed-loop generator cannot observe the requests it failed to send while \
         blocked on a slow response. The corrected columns apply HdrHistogram's \
         `record_correct` back-fill against the run's pacing interval, which \
         reconstructs those missing samples.\n\n\
         Correction moves the tail *up* when slowness is bursty — a few long stalls \
         hide many requests that were never issued — and *down* when the whole run is \
         uniformly saturated, because then the back-filled samples are spread below the \
         observed latency rather than piled at it. Both are the same computation; the \
         direction tells you which regime the candidate was in.\n\n\
         Raw and corrected are identical for closed-loop runs (no `--target-rps`), \
         where there is no pacing interval to correct against.\n\n",
    );
    md.push_str(
        "| Candidate | Conn | p99 raw | p99 corrected | p99.9 raw | p99.9 corrected | p99.99 | max |\n\
         | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for r in results {
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            r.candidate_name,
            r.concurrency,
            micros(r.latency_p99_us),
            micros(r.co_corrected_p99_us),
            micros(r.latency_p999_us),
            micros(r.co_corrected_p999_us),
            micros(r.latency_p9999_us),
            micros(r.latency_max_us),
        );
    }
    md.push('\n');
    md
}

/// CPU and memory, attributed to the proxy process alone.
fn resource_table(results: &[BenchmarkResult]) -> String {
    let mut md = String::from("## Resource Footprint\n\n");
    md.push_str(
        "Sampled from the candidate's own process (and its children), not from the \
         benchmark harness.\n\n",
    );
    md.push_str(
        "| Candidate | Conn | CPU (mean) | Peak RSS | RSS per 1k conn |\n\
         | :--- | ---: | ---: | ---: | ---: |\n",
    );
    for r in results {
        let per_1k = if r.concurrency > 0 {
            r.rss_memory_mb / (r.concurrency as f64 / 1000.0)
        } else {
            0.0
        };
        let _ = writeln!(
            md,
            "| {} | {} | {:.1}% | {:.1} MiB | {:.2} MiB |",
            r.candidate_name, r.concurrency, r.cpu_usage_pct, r.rss_memory_mb, per_1k
        );
    }
    md.push('\n');
    md
}

/// Flag any run whose numbers should not be trusted.
fn integrity_section(results: &[BenchmarkResult]) -> String {
    let dirty: Vec<&BenchmarkResult> = results.iter().filter(|r| !r.is_clean()).collect();
    if dirty.is_empty() {
        return "## Run Integrity\n\nAll measurements completed with zero transport failures \
                and zero error responses.\n\n"
            .to_string();
    }

    let mut md = String::from("## Run Integrity\n\n");
    md.push_str(
        "> [!WARNING]\n> The runs below recorded failures or error responses. Their \
         throughput figures are not comparable with clean runs, because a request that \
         404s costs the proxy far less than one it forwards.\n\n",
    );
    md.push_str(
        "| Candidate | Conn | Failed | 4xx/5xx | Share |\n| :--- | ---: | ---: | ---: | ---: |\n",
    );
    for r in dirty {
        let share = if r.total_requests > 0 {
            (r.failed_requests + r.error_responses) as f64 / r.total_requests as f64 * 100.0
        } else {
            0.0
        };
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} | {share:.2}% |",
            r.candidate_name, r.concurrency, r.failed_requests, r.error_responses
        );
    }
    md.push('\n');
    md
}

/// A PGO standard-vs-optimised comparison section (plan §8, Scenario 5).
#[must_use]
pub fn pgo_delta_section(results: &[BenchmarkResult]) -> String {
    let pairs = crate::harness::pair_pgo_results(results);
    if pairs.is_empty() {
        return String::new();
    }
    let mut md = String::from("## PGO Impact Delta\n\n");
    md.push_str(
        "| Workload | Standard RPS | PGO RPS | Δ | Standard p99 | PGO p99 |\n\
         | :--- | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for (standard, pgo, delta) in &pairs {
        let _ = writeln!(
            md,
            "| c{} / {} / {} | {:.0} | {:.0} | **{:+.1}%** | {} | {} |",
            standard.concurrency,
            human_bytes(standard.payload_bytes),
            standard.http_version,
            standard.rps,
            pgo.rps,
            delta,
            micros(standard.latency_p99_us),
            micros(pgo.latency_p99_us),
        );
    }
    let mean: f64 = pairs.iter().map(|(_, _, d)| *d).sum::<f64>() / pairs.len() as f64;
    let _ = writeln!(md, "\nMean throughput change from PGO: **{mean:+.1}%**\n");
    md
}

/// Link every captured flamegraph, relative to the report.
#[must_use]
pub fn flamegraph_section(paths: &[std::path::PathBuf]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let mut md = String::from("## Flamegraphs\n\n");
    for path in paths {
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "profile".into());
        let rel = relative_to_report(path);
        let _ = writeln!(md, "- [{name}]({rel})");
    }
    md.push('\n');
    md
}

/// Flamegraphs live in `<run>/flamegraphs/`, next to `report.md`.
fn relative_to_report(path: &Path) -> String {
    match path.file_name() {
        Some(name) => format!("flamegraphs/{}", name.to_string_lossy()),
        None => path.display().to_string(),
    }
}

/// Render a byte count compactly.
fn human_bytes(bytes: usize) -> String {
    match bytes {
        0 => "-".to_string(),
        b if b >= 1024 * 1024 => format!("{} MiB", b / (1024 * 1024)),
        b if b >= 1024 => format!("{} KiB", b / 1024),
        b => format!("{b} B"),
    }
}

/// Render microseconds in the most readable unit.
fn micros(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.2} s", us as f64 / 1_000_000.0)
    } else if us >= 1_000 {
        format!("{:.2} ms", us as f64 / 1_000.0)
    } else {
        format!("{us} µs")
    }
}

/// Percentage improvement helper, re-exported for report consumers.
#[must_use]
pub fn improvement(standard: &BenchmarkResult, candidate: &BenchmarkResult) -> f64 {
    pgo_improvement_pct(standard, candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(name: &str, rps: f64) -> BenchmarkResult {
        BenchmarkResult {
            candidate_name: name.into(),
            scenario_name: "Throughput".into(),
            concurrency: 1_000,
            payload_bytes: 1024,
            http_version: "HTTP/1.1".into(),
            total_requests: 1_000,
            successful_requests: 1_000,
            failed_requests: 0,
            error_responses: 0,
            bytes_received: 1_024_000,
            duration_secs: 10.0,
            rps,
            throughput_mib_s: 0.1,
            latency_p50_us: 500,
            latency_p90_us: 900,
            latency_p95_us: 1_200,
            latency_p99_us: 2_500,
            latency_p999_us: 9_000,
            latency_p9999_us: 40_000,
            latency_max_us: 1_500_000,
            latency_mean_us: 600.0,
            latency_stdev_us: 120.0,
            co_corrected_p99_us: 8_000,
            co_corrected_p999_us: 90_000,
            cpu_usage_pct: 250.0,
            rss_memory_mb: 64.0,
        }
    }

    #[test]
    fn report_contains_every_required_section() {
        let hw = HardwareSpec::detect();
        let md = generate_markdown_report(&hw, &[result("lb-proxy-hyper", 100_000.0)]);
        for section in [
            "Hardware & Environment",
            "Throughput & Latency",
            "Tail Latency",
            "Resource Footprint",
            "Run Integrity",
        ] {
            assert!(md.contains(section), "report is missing '{section}'");
        }
    }

    #[test]
    fn toolchain_is_taken_from_detection_not_hardcoded() {
        let hw = HardwareSpec::detect();
        let md = generate_markdown_report(&hw, &[result("x", 1.0)]);
        assert!(md.contains(&hw.rust_version));
        // The old report hardcoded this string regardless of the real toolchain.
        assert!(
            !md.contains("1.97.1 (2024 Edition)") || hw.rust_version.contains("1.97.1"),
            "toolchain string must come from detection"
        );
    }

    #[test]
    fn empty_results_produce_a_valid_report() {
        let md = generate_markdown_report(&HardwareSpec::detect(), &[]);
        assert!(md.contains("No measurements"));
    }

    #[test]
    fn dirty_runs_are_flagged_with_a_warning() {
        let mut bad = result("lb-proxy-monoio", 500_000.0);
        bad.failed_requests = 120;
        bad.error_responses = 30;
        let md = generate_markdown_report(&HardwareSpec::detect(), &[bad]);
        assert!(md.contains("WARNING"), "{md}");
        assert!(md.contains("15.00%"), "failure share should be shown: {md}");
    }

    #[test]
    fn clean_runs_say_so_explicitly() {
        let md = generate_markdown_report(&HardwareSpec::detect(), &[result("a", 1.0)]);
        assert!(md.contains("zero transport failures"));
    }

    #[test]
    fn pgo_section_pairs_and_averages() {
        let mut standard = result("h [standard]", 1_000.0);
        standard.candidate_name = "h [standard]".into();
        let mut pgo = result("h [pgo]", 1_100.0);
        pgo.candidate_name = "h [pgo]".into();
        let md = pgo_delta_section(&[standard, pgo]);
        assert!(md.contains("+10.0%"), "{md}");
        assert!(md.contains("Mean throughput change"));
    }

    #[test]
    fn pgo_section_is_empty_without_paired_runs() {
        assert!(pgo_delta_section(&[result("plain", 1.0)]).is_empty());
    }

    #[test]
    fn units_are_rendered_readably() {
        assert_eq!(micros(999), "999 µs");
        assert_eq!(micros(2_500), "2.50 ms");
        assert_eq!(micros(1_500_000), "1.50 s");
        assert_eq!(human_bytes(1024), "1 KiB");
        assert_eq!(human_bytes(1024 * 1024), "1 MiB");
        assert_eq!(human_bytes(0), "-");
    }

    #[test]
    fn flamegraph_links_are_relative_to_the_report() {
        let md = flamegraph_section(&[std::path::PathBuf::from(
            "/abs/results/run_x/flamegraphs/lb-proxy-hyper-c1000.svg",
        )]);
        assert!(
            md.contains("(flamegraphs/lb-proxy-hyper-c1000.svg)"),
            "{md}"
        );
        assert!(!md.contains("/abs/"), "links must not be absolute: {md}");
    }

    #[test]
    fn resource_table_normalises_per_thousand_connections() {
        let md = generate_markdown_report(&HardwareSpec::detect(), &[result("x", 1.0)]);
        // 64 MiB at 1000 connections.
        assert!(md.contains("64.00 MiB"), "{md}");
    }
}
