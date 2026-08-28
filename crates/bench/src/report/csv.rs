//! CSV export of raw metrics.

use crate::metrics::BenchmarkResult;
use std::fmt::Write;

/// Column header, kept in sync with [`export_csv`] by a test below.
pub const CSV_HEADER: &str = "candidate,scenario,concurrency,payload_bytes,http_version,\
total_requests,successful,failed,error_responses,bytes_received,duration_s,rps,throughput_mib_s,\
p50_us,p90_us,p95_us,p99_us,p999_us,p9999_us,max_us,mean_us,stdev_us,\
co_p99_us,co_p999_us,cpu_pct,rss_mb";

/// Render results as CSV.
#[must_use]
pub fn export_csv(results: &[BenchmarkResult]) -> String {
    let mut csv = String::with_capacity(CSV_HEADER.len() + results.len() * 160);
    csv.push_str(CSV_HEADER);
    csv.push('\n');
    for r in results {
        let _ = writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{:.4},{:.2},{:.4},\
             {},{},{},{},{},{},{},{:.2},{:.2},{},{},{:.2},{:.2}",
            escape(&r.candidate_name),
            escape(&r.scenario_name),
            r.concurrency,
            r.payload_bytes,
            escape(&r.http_version),
            r.total_requests,
            r.successful_requests,
            r.failed_requests,
            r.error_responses,
            r.bytes_received,
            r.duration_secs,
            r.rps,
            r.throughput_mib_s,
            r.latency_p50_us,
            r.latency_p90_us,
            r.latency_p95_us,
            r.latency_p99_us,
            r.latency_p999_us,
            r.latency_p9999_us,
            r.latency_max_us,
            r.latency_mean_us,
            r.latency_stdev_us,
            r.co_corrected_p99_us,
            r.co_corrected_p999_us,
            r.cpu_usage_pct,
            r.rss_memory_mb
        );
    }
    csv
}

/// Quote a field that contains a comma or a quote, per RFC 4180.
///
/// Scenario names contain commas ("Route Churn & Jitter, 100 Hz"), which would
/// otherwise shift every subsequent column.
fn escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BenchmarkResult {
        BenchmarkResult {
            candidate_name: "lb-proxy-hyper".into(),
            scenario_name: "Throughput, full sweep".into(),
            concurrency: 1_000,
            payload_bytes: 1024,
            http_version: "HTTP/1.1".into(),
            total_requests: 100,
            successful_requests: 98,
            failed_requests: 2,
            error_responses: 1,
            bytes_received: 102_400,
            duration_secs: 10.0,
            rps: 10.0,
            throughput_mib_s: 0.0097,
            latency_p50_us: 100,
            latency_p90_us: 200,
            latency_p95_us: 250,
            latency_p99_us: 300,
            latency_p999_us: 400,
            latency_p9999_us: 500,
            latency_max_us: 600,
            latency_mean_us: 150.0,
            latency_stdev_us: 25.0,
            co_corrected_p99_us: 350,
            co_corrected_p999_us: 450,
            cpu_usage_pct: 55.5,
            rss_memory_mb: 42.25,
        }
    }

    #[test]
    fn header_column_count_matches_the_rows() {
        let csv = export_csv(&[sample()]);
        let mut lines = csv.lines();
        let header_cols = lines.next().unwrap().split(',').count();
        let row = lines.next().unwrap();
        // Count fields respecting the single quoted field in the sample.
        let row_cols = split_csv_row(row).len();
        assert_eq!(header_cols, row_cols, "header/row column mismatch:\n{csv}");
    }

    #[test]
    fn commas_in_names_are_quoted_not_leaked() {
        let csv = export_csv(&[sample()]);
        assert!(
            csv.contains("\"Throughput, full sweep\""),
            "scenario name must be quoted: {csv}"
        );
        let row = csv.lines().nth(1).unwrap();
        let fields = split_csv_row(row);
        assert_eq!(fields[1], "Throughput, full sweep");
        assert_eq!(fields[2], "1000", "column alignment broke");
    }

    #[test]
    fn embedded_quotes_are_doubled() {
        let mut r = sample();
        r.candidate_name = "he said \"hi\"".into();
        let csv = export_csv(&[r]);
        assert!(csv.contains("\"he said \"\"hi\"\"\""), "{csv}");
    }

    #[test]
    fn empty_results_still_emit_the_header() {
        let csv = export_csv(&[]);
        assert_eq!(csv.trim(), CSV_HEADER);
    }

    #[test]
    fn coordinated_omission_columns_are_present() {
        let csv = export_csv(&[sample()]);
        assert!(csv.contains("co_p99_us"));
        assert!(csv.contains("co_p999_us"));
    }

    /// Minimal RFC 4180 row splitter, for assertions only.
    fn split_csv_row(row: &str) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = row.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '"' if in_quotes && chars.peek() == Some(&'"') => {
                    current.push('"');
                    chars.next();
                }
                '"' => in_quotes = !in_quotes,
                ',' if !in_quotes => fields.push(std::mem::take(&mut current)),
                other => current.push(other),
            }
        }
        fields.push(current);
        fields
    }
}
