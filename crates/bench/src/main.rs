//! `lb-bench` CLI.

use clap::{Parser, ValueEnum};
use lb_bench::harness::candidate::default_binary_dir;
use lb_bench::{
    BenchmarkRunner, Candidate, Deployment, FlamegraphCapture, FlamegraphConfig, HardwareSpec,
    HttpVersion, PayloadSize, PgoConfig, PgoPipeline, RunDirectory, RunnerConfig, ScenarioConfig,
    ScenarioKind, WarmupProtocol, export_csv, generate_markdown_report, pgo_delta_section,
    write_hardware_spec,
};
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Launch candidates locally as child processes.
    Local,
    /// Drive candidates already deployed to a kind cluster.
    K8s,
}

#[derive(Parser, Debug)]
#[command(
    name = "lb-bench",
    version,
    about = "Reverse proxy benchmark suite (Hyper / Pingora / Monoio vs. Traefik)"
)]
struct Args {
    /// Where the candidates run.
    #[arg(long, value_enum, default_value_t = Mode::Local)]
    mode: Mode,

    /// Candidate to benchmark. Repeatable.
    #[arg(long, value_delimiter = ',')]
    candidate: Vec<String>,

    /// Benchmark every candidate.
    #[arg(long)]
    all: bool,

    /// Scenario: throughput, connection-density, route-churn, pgo-delta.
    #[arg(long, default_value = "throughput")]
    scenario: String,

    /// Concurrency levels. Overrides the scenario default.
    #[arg(long, value_delimiter = ',')]
    concurrency: Vec<usize>,

    /// Payload sizes: 1k, 64k, 1m. Overrides the scenario default.
    #[arg(long, value_delimiter = ',')]
    payload_sizes: Vec<String>,

    /// HTTP versions: h1, h2. Overrides the scenario default.
    #[arg(long, value_delimiter = ',')]
    http: Vec<String>,

    /// Measurement window, e.g. `30s`, `2m`.
    #[arg(long, default_value = "30s")]
    duration: String,

    /// Warm-up traffic duration, e.g. `15s`.
    #[arg(long, default_value = "15s")]
    warmup: String,

    /// Fixed offered load in RPS. Omit for a closed-loop run.
    #[arg(long)]
    target_rps: Option<u64>,

    /// HTTPRoute mutations per second (route-churn scenario).
    #[arg(long, default_value_t = 0)]
    churn_rate: u32,

    /// Mock upstream response delay in ms (0 / 1 / 5 profiles).
    #[arg(long, default_value_t = 0)]
    upstream_delay_ms: u64,

    /// Worker threads per candidate. Defaults to the logical CPU count.
    #[arg(long)]
    workers: Option<usize>,

    /// Directory holding the candidate binaries.
    #[arg(long)]
    binary_dir: Option<PathBuf>,

    /// Route config passed to locally launched candidates.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Address of an already-running candidate (skips launching).
    #[arg(long)]
    target: Option<String>,

    /// Results root directory.
    #[arg(long, default_value = "results")]
    output_dir: PathBuf,

    /// Capture a CPU flamegraph per candidate.
    #[arg(long)]
    flamegraph: bool,

    /// Run the three-pass PGO build before benchmarking, then compare.
    #[arg(long)]
    pgo: bool,

    /// Use the short smoke-test sweep.
    #[arg(long)]
    quick: bool,
}

/// Parse a duration such as `30s`, `500ms`, `2m`.
fn parse_duration(text: &str) -> Result<Duration, anyhow::Error> {
    let text = text.trim();
    let (value, unit) = text
        .find(|c: char| c.is_ascii_alphabetic())
        .map_or((text, "s"), |i| text.split_at(i));
    let value: f64 = value
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid duration: {text}"))?;
    let secs = match unit {
        "ms" => value / 1000.0,
        "s" | "" => value,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        other => anyhow::bail!("unknown duration unit '{other}' in '{text}'"),
    };
    Ok(Duration::from_secs_f64(secs))
}

fn build_scenario(args: &Args) -> Result<ScenarioConfig, anyhow::Error> {
    let kind = ScenarioKind::parse(&args.scenario)
        .ok_or_else(|| anyhow::anyhow!("unknown scenario '{}'", args.scenario))?;

    let mut scenario = if args.quick {
        ScenarioConfig::quick()
    } else {
        match kind {
            ScenarioKind::Throughput => ScenarioConfig::throughput(),
            ScenarioKind::ConnectionDensity => ScenarioConfig::connection_density(),
            ScenarioKind::RouteChurn => ScenarioConfig::route_churn(args.churn_rate.max(10)),
            ScenarioKind::PgoDelta => ScenarioConfig::pgo_delta(),
        }
    };

    if !args.concurrency.is_empty() {
        scenario.concurrencies = args.concurrency.clone();
    }
    if !args.payload_sizes.is_empty() {
        scenario.payloads = args
            .payload_sizes
            .iter()
            .map(|s| {
                PayloadSize::parse(s).ok_or_else(|| anyhow::anyhow!("unknown payload size '{s}'"))
            })
            .collect::<Result<Vec<_>, _>>()?;
    }
    if !args.http.is_empty() {
        scenario.http_versions = args
            .http
            .iter()
            .map(|s| {
                HttpVersion::parse(s).ok_or_else(|| anyhow::anyhow!("unknown HTTP version '{s}'"))
            })
            .collect::<Result<Vec<_>, _>>()?;
    }
    if !args.quick {
        scenario.duration = parse_duration(&args.duration)?;
    }
    if let Some(rps) = args.target_rps {
        scenario.target_rps = Some(rps);
    }
    if args.churn_rate > 0 {
        scenario.route_churn_rate_hz = args.churn_rate;
    }
    Ok(scenario)
}

fn selected_candidates(args: &Args) -> Result<Vec<Candidate>, anyhow::Error> {
    if args.all {
        return Ok(match args.mode {
            // Traefik only exists inside the cluster.
            Mode::Local => Candidate::rust_candidates().to_vec(),
            Mode::K8s => Candidate::all().to_vec(),
        });
    }
    if args.candidate.is_empty() {
        return Ok(vec![Candidate::Hyper]);
    }
    args.candidate
        .iter()
        .map(|name| {
            Candidate::parse(name).ok_or_else(|| anyhow::anyhow!("unknown candidate '{name}'"))
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let hardware = HardwareSpec::detect();
    info!(
        cpu = %hardware.cpu_model,
        cores = hardware.cpu_cores_physical,
        ram_gb = format_args!("{:.1}", hardware.ram_total_gb),
        rustc = %hardware.rust_version,
        "environment detected"
    );

    let scenario = build_scenario(&args)?;
    let candidates = selected_candidates(&args)?;
    let binary_dir = args.binary_dir.clone().unwrap_or_else(default_binary_dir);

    info!(
        scenario = %scenario.name,
        candidates = ?candidates.iter().copied().map(Candidate::display_name).collect::<Vec<_>>(),
        windows = scenario.measurement_count(),
        "starting benchmark"
    );

    let deployment = match (&args.target, args.mode) {
        (Some(addr), _) => Deployment::External {
            addr: addr.parse()?,
            pid: None,
        },
        (None, Mode::Local) => Deployment::Local {
            binary_dir: binary_dir.clone(),
            workers: args.workers,
        },
        (None, Mode::K8s) => Deployment::External {
            // Port 0 tells the runner to resolve each candidate's own kind
            // host port, rather than sending every candidate's load to one.
            addr: "127.0.0.1:0".parse()?,
            pid: None,
        },
    };

    let warmup = if args.quick {
        WarmupProtocol::quick()
    } else {
        WarmupProtocol {
            traffic: parse_duration(&args.warmup)?,
            ..WarmupProtocol::default()
        }
    };

    let runner = BenchmarkRunner::with_config(RunnerConfig {
        warmup,
        deployment,
        upstream_addr: None,
        upstream_delay_ms: args.upstream_delay_ms,
        config_path: args.config.clone(),
    });

    // Optional PGO comparison run (plan §8, Scenario 5).
    let mut results = if args.pgo {
        let candidate = *candidates.first().unwrap_or(&Candidate::Hyper);
        info!(
            candidate = candidate.display_name(),
            "running the PGO pipeline"
        );
        let pipeline = PgoPipeline::new(PgoConfig {
            route_config: args
                .config
                .clone()
                .unwrap_or_else(|| PathBuf::from("examples/pgo_routes.yaml")),
            ..PgoConfig::default()
        });

        // Preserve the standard build before the PGO passes overwrite it.
        let standard_dir = binary_dir
            .parent()
            .unwrap_or(&binary_dir)
            .join("pgo-baseline");
        std::fs::create_dir_all(&standard_dir)?;
        if let Some(name) = candidate.binary_name() {
            std::fs::copy(binary_dir.join(name), standard_dir.join(name))?;
        }

        pipeline.run(candidate).await?;
        runner
            .run_pgo_delta(candidate, standard_dir, binary_dir.clone())
            .await?
    } else {
        runner.run_all(&candidates, &scenario).await
    };
    results.sort_by(|a, b| {
        a.candidate_name
            .cmp(&b.candidate_name)
            .then(a.concurrency.cmp(&b.concurrency))
    });

    // Optional flamegraph capture.
    let flamegraph_paths = if args.flamegraph {
        let capture = FlamegraphCapture::new(FlamegraphConfig::default());
        if !FlamegraphCapture::profiler_available().await {
            warn!("no profiler available; skipping flamegraphs");
        }
        capture.collected()
    } else {
        Vec::new()
    };

    // Write the run directory (plan §5 layout).
    let run_dir = RunDirectory::create(&args.output_dir, &hardware.timestamp_utc)?;
    write_hardware_spec(&run_dir, &hardware)?;
    std::fs::write(
        run_dir.raw_metrics_json(),
        serde_json::to_string_pretty(&results)?,
    )?;
    std::fs::write(run_dir.results_csv(), export_csv(&results))?;

    let mut report = generate_markdown_report(&hardware, &results);
    report.push_str(&pgo_delta_section(&results));
    report.push_str(&lb_bench::report::flamegraph_section(&flamegraph_paths));
    std::fs::write(run_dir.report_md(), &report)?;

    println!("\n{report}");
    info!(dir = %run_dir.root.display(), "results written");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_parsing_covers_the_cli_forms() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("15").unwrap(), Duration::from_secs(15));
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10x").is_err());
    }

    fn args_from(argv: &[&str]) -> Args {
        Args::parse_from(std::iter::once("lb-bench").chain(argv.iter().copied()))
    }

    #[test]
    fn candidate_selection_honours_the_flags() {
        let args = args_from(&["--candidate", "pingora,monoio"]);
        assert_eq!(
            selected_candidates(&args).unwrap(),
            vec![Candidate::Pingora, Candidate::Monoio]
        );

        let all_local = args_from(&["--all"]);
        assert_eq!(selected_candidates(&all_local).unwrap().len(), 3);

        let all_k8s = args_from(&["--all", "--mode", "k8s"]);
        assert!(
            selected_candidates(&all_k8s)
                .unwrap()
                .contains(&Candidate::Traefik),
            "k8s mode must include the Traefik baseline"
        );
    }

    #[test]
    fn unknown_candidate_is_rejected_with_its_name() {
        let args = args_from(&["--candidate", "envoy"]);
        assert!(
            selected_candidates(&args)
                .unwrap_err()
                .to_string()
                .contains("envoy")
        );
    }

    #[test]
    fn cli_overrides_reach_the_scenario() {
        let args = args_from(&[
            "--concurrency",
            "100,1000",
            "--payload-sizes",
            "1k,64k",
            "--http",
            "h1,h2",
            "--duration",
            "45s",
        ]);
        let s = build_scenario(&args).unwrap();
        assert_eq!(s.concurrencies, vec![100, 1_000]);
        assert_eq!(
            s.payloads,
            vec![PayloadSize::Small1Kb, PayloadSize::Medium64Kb]
        );
        assert_eq!(s.http_versions.len(), 2);
        assert_eq!(s.duration, Duration::from_secs(45));
        assert_eq!(s.measurement_count(), 2 * 2 * 2);
    }

    #[test]
    fn scenario_selection_matches_the_plan_definitions() {
        let density = build_scenario(&args_from(&["--scenario", "connection-density"])).unwrap();
        assert_eq!(density.kind, ScenarioKind::ConnectionDensity);
        assert_eq!(density.concurrencies, vec![10_000, 25_000, 50_000]);

        let churn = build_scenario(&args_from(&[
            "--scenario",
            "route-churn",
            "--churn-rate",
            "250",
        ]))
        .unwrap();
        assert_eq!(churn.route_churn_rate_hz, 250);
    }

    #[test]
    fn unknown_scenario_and_payload_are_rejected() {
        assert!(build_scenario(&args_from(&["--scenario", "nonsense"])).is_err());
        assert!(build_scenario(&args_from(&["--payload-sizes", "7k"])).is_err());
        assert!(build_scenario(&args_from(&["--http", "h3"])).is_err());
    }

    #[test]
    fn quick_mode_shortens_the_sweep() {
        let s = build_scenario(&args_from(&["--quick"])).unwrap();
        assert!(s.duration <= Duration::from_secs(5));
        assert!(s.measurement_count() <= 4);
    }

    #[test]
    fn default_run_benchmarks_hyper_locally() {
        let args = args_from(&[]);
        assert_eq!(args.mode, Mode::Local);
        assert_eq!(selected_candidates(&args).unwrap(), vec![Candidate::Hyper]);
    }
}
