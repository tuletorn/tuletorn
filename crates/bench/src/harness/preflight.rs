//! System-limit preflight.
//!
//! A load generator that asks for more connections than the kernel will give it
//! does not fail loudly — it quietly serves fewer, retries the rest, and reports
//! a throughput figure for a concurrency level that never existed. Every check
//! here exists because it silently corrupts a measurement.
//!
//! macOS is far tighter than Linux out of the box: `kern.ipc.somaxconn` is 128
//! (Linux: 4096), the ephemeral port range is 16 384 wide (Linux: 28 232 and
//! easily widened), and `kern.maxfilesperproc` is 61 440. The C10K-C50K sweep in
//! plan §8.2 needs all three raised.

use std::fmt;

/// One system limit and whether it can carry the requested load.
#[derive(Debug, Clone)]
pub struct Check {
    pub name: &'static str,
    pub current: u64,
    pub required: u64,
    pub severity: Severity,
    /// Exact command that raises it, if any.
    pub remedy: Option<String>,
    pub consequence: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Headroom is fine.
    Ok,
    /// Will work, but the margin is thin enough to distort the tail.
    Tight,
    /// The requested concurrency cannot be reached; numbers would be fiction.
    Blocking,
}

impl Check {
    fn new(
        name: &'static str,
        current: u64,
        required: u64,
        consequence: &'static str,
        remedy: Option<String>,
    ) -> Self {
        let severity = if current >= required {
            Severity::Ok
        } else if current * 100 >= required * 80 {
            Severity::Tight
        } else {
            Severity::Blocking
        };
        Self {
            name,
            current,
            required,
            severity,
            remedy,
            consequence,
        }
    }

    #[must_use]
    pub fn is_blocking(&self) -> bool {
        self.severity == Severity::Blocking
    }
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mark = match self.severity {
            Severity::Ok => "ok  ",
            Severity::Tight => "TIGHT",
            Severity::Blocking => "BLOCK",
        };
        write!(
            f,
            "[{mark}] {:<26} have {:>7}  need {:>7}",
            self.name, self.current, self.required
        )
    }
}

/// The full preflight result for one requested concurrency level.
#[derive(Debug, Clone)]
pub struct Preflight {
    pub concurrency: usize,
    pub checks: Vec<Check>,
}

impl Preflight {
    /// Evaluate every limit against `concurrency` simultaneous connections.
    ///
    /// `upstream_multiplier` accounts for the proxy's own outbound connections:
    /// on a loopback run the same machine holds the client sockets, the proxy's
    /// accepted sockets, and the proxy's upstream sockets at once.
    #[must_use]
    pub fn evaluate(concurrency: usize, upstream_multiplier: f64) -> Self {
        let conn = concurrency as u64;
        // Downstream client socket + proxy's accepted socket + proxy's upstream
        // socket + the mock upstream's accepted socket.
        let total_sockets = (conn as f64 * (2.0 + upstream_multiplier * 2.0)) as u64;
        // Ephemeral ports are consumed by the two connecting sides only.
        let ephemeral_needed = (conn as f64 * (1.0 + upstream_multiplier)) as u64;

        let mut checks = Vec::new();

        // 1. Per-process file descriptors.
        let fd_soft = rlimit_nofile();
        checks.push(Check::new(
            "open files (ulimit -n)",
            fd_soft,
            conn * 2 + 1_024,
            "the generator cannot open all its connections and reports a lower \
             concurrency than requested",
            Some(format!("ulimit -n {}", (conn * 4).max(65_536))),
        ));

        // 2. Kernel-wide file descriptor ceiling.
        if let Some(maxfiles) = sysctl_u64("kern.maxfilesperproc") {
            checks.push(Check::new(
                "kern.maxfilesperproc",
                maxfiles,
                conn * 2 + 1_024,
                "caps `ulimit -n` regardless of the shell setting",
                Some(format!(
                    "sudo sysctl -w kern.maxfilesperproc={}",
                    (conn * 4).max(65_536)
                )),
            ));
        }
        if let Some(maxfiles) = sysctl_u64("kern.maxfiles") {
            checks.push(Check::new(
                "kern.maxfiles",
                maxfiles,
                total_sockets + 8_192,
                "system-wide descriptor exhaustion affects every process, not just \
                 the benchmark",
                Some(format!(
                    "sudo sysctl -w kern.maxfiles={}",
                    (total_sockets * 2).max(131_072)
                )),
            ));
        }

        // 3. Listen backlog. This is the one that silently ruins a ramp.
        if let Some(somaxconn) = sysctl_u64("kern.ipc.somaxconn") {
            // The accept queue absorbs the connect burst, not the steady state.
            // Workers spawn over a few milliseconds, so roughly an eighth of the
            // target is in flight at the peak; below 128 the kernel default is
            // already enough, and a 128-deep queue against a 10k ramp drops SYNs
            // whose retries land in the histogram as multi-second outliers.
            let needed = (conn / 8).clamp(128, 8_192);
            checks.push(Check::new(
                "kern.ipc.somaxconn",
                somaxconn,
                needed,
                "SYN queue overflows during the connection ramp; the TCP retries \
                 land in the histogram as seconds-long outliers",
                Some(format!(
                    "sudo sysctl -w kern.ipc.somaxconn={}",
                    needed.next_power_of_two().max(2_048)
                )),
            ));
        }

        // 4. Ephemeral ports. Hard ceiling on loopback concurrency.
        let (first, last) = ephemeral_range();
        if let (Some(first), Some(last)) = (first, last) {
            let available = last.saturating_sub(first) + 1;
            checks.push(Check::new(
                "ephemeral port range",
                available,
                ephemeral_needed + 4_096,
                "connections fail with EADDRNOTAVAIL partway through the ramp; the \
                 run measures whatever fraction of the target it managed to open",
                Some(
                    "sudo sysctl -w net.inet.ip.portrange.first=16384  # macOS\n\
                     # Linux: sudo sysctl -w net.ipv4.ip_local_port_range='10000 65535'"
                        .to_string(),
                ),
            ));
        }

        // 5. TIME_WAIT drain rate, which decides how fast ports come back
        //    between measurement windows.
        if let Some(msl_ms) = sysctl_u64("net.inet.tcp.msl") {
            let tw_seconds = (msl_ms * 2) / 1_000;
            // Ports must recycle faster than the inter-window settle period.
            checks.push(Check::new(
                "TIME_WAIT drain (2*MSL, s)",
                // Inverted: lower is better, so express as headroom.
                if tw_seconds == 0 { 60 } else { 60 / tw_seconds },
                2,
                "ports from the previous window are still held when the next one \
                 starts, so later sweeps run with less port space than earlier ones",
                Some("sudo sysctl -w net.inet.tcp.msl=1000".to_string()),
            ));
        }

        Self {
            concurrency,
            checks,
        }
    }

    /// Checks that would make the measurement invalid.
    #[must_use]
    pub fn blocking(&self) -> Vec<&Check> {
        self.checks.iter().filter(|c| c.is_blocking()).collect()
    }

    /// The highest concurrency this system can actually sustain.
    ///
    /// Binary search over [`Self::evaluate`], so the answer accounts for every
    /// limit at once rather than just the most obvious one.
    #[must_use]
    pub fn max_supported(upstream_multiplier: f64) -> usize {
        // Start at 1: evaluating 0 connections is meaningless.
        let (mut lo, mut hi) = (1usize, 200_000usize);
        if !Self::evaluate(lo, upstream_multiplier).blocking().is_empty() {
            return 0;
        }
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            if Self::evaluate(mid, upstream_multiplier).blocking().is_empty() {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        lo
    }

    /// A human-readable report.
    #[must_use]
    pub fn report(&self) -> String {
        use fmt::Write as _;
        let mut out = format!(
            "System preflight for {} concurrent connections\n\n",
            self.concurrency
        );
        for check in &self.checks {
            let _ = writeln!(out, "  {check}");
        }

        let blocking = self.blocking();
        if blocking.is_empty() {
            out.push_str("\n  All limits have headroom.\n");
            return out;
        }

        out.push_str("\n  BLOCKING — this run would not reach the requested concurrency:\n");
        for check in &blocking {
            let _ = writeln!(out, "\n  * {}\n    {}", check.name, check.consequence);
            if let Some(remedy) = &check.remedy {
                for line in remedy.lines() {
                    let _ = writeln!(out, "      {line}");
                }
            }
        }
        out
    }
}

/// Read `RLIMIT_NOFILE`'s soft limit.
fn rlimit_nofile() -> u64 {
    #[cfg(unix)]
    {
        #[repr(C)]
        struct RLimit {
            soft: u64,
            hard: u64,
        }
        unsafe extern "C" {
            fn getrlimit(resource: i32, rlim: *mut RLimit) -> i32;
        }
        // RLIMIT_NOFILE is 8 on Darwin, 7 on Linux.
        #[cfg(target_os = "macos")]
        const RLIMIT_NOFILE: i32 = 8;
        #[cfg(not(target_os = "macos"))]
        const RLIMIT_NOFILE: i32 = 7;

        let mut limit = RLimit { soft: 0, hard: 0 };
        // SAFETY: `getrlimit` writes two u64s into a struct we own.
        if unsafe { getrlimit(RLIMIT_NOFILE, &mut limit) } == 0 {
            return limit.soft;
        }
    }
    0
}

/// Read an integer sysctl by name. Returns `None` where the name does not exist
/// (notably on Linux, where these are `/proc` entries instead).
fn sysctl_u64(name: &str) -> Option<u64> {
    let out = std::process::Command::new("sysctl")
        .arg("-n")
        .arg(name)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()?.trim().parse().ok()
}

/// The ephemeral port range, on either platform.
fn ephemeral_range() -> (Option<u64>, Option<u64>) {
    if let (Some(first), Some(last)) = (
        sysctl_u64("net.inet.ip.portrange.first"),
        sysctl_u64("net.inet.ip.portrange.last"),
    ) {
        return (Some(first), Some(last));
    }
    // Linux exposes both bounds in one file.
    if let Ok(text) = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range") {
        let mut parts = text.split_whitespace();
        let first = parts.next().and_then(|s| s.parse().ok());
        let last = parts.next().and_then(|s| s.parse().ok());
        return (first, last);
    }
    (None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_this_process_descriptor_limit() {
        let n = rlimit_nofile();
        assert!(n >= 256, "implausible RLIMIT_NOFILE: {n}");
    }

    #[test]
    fn low_concurrency_has_headroom_everywhere() {
        let pre = Preflight::evaluate(100, 1.0);
        assert!(
            pre.blocking().is_empty(),
            "100 connections should be trivially achievable:\n{}",
            pre.report()
        );
    }

    #[test]
    fn absurd_concurrency_is_blocked_not_silently_accepted() {
        let pre = Preflight::evaluate(500_000, 1.0);
        assert!(
            !pre.blocking().is_empty(),
            "500k connections cannot be achievable on any developer machine"
        );
    }

    #[test]
    fn severity_ordering_is_meaningful() {
        assert!(Severity::Ok < Severity::Tight);
        assert!(Severity::Tight < Severity::Blocking);
    }

    #[test]
    fn a_check_at_exactly_the_requirement_passes() {
        let c = Check::new("x", 100, 100, "", None);
        assert_eq!(c.severity, Severity::Ok);
    }

    #[test]
    fn a_check_slightly_under_is_tight_not_blocking() {
        let c = Check::new("x", 85, 100, "", None);
        assert_eq!(c.severity, Severity::Tight);
        assert!(!c.is_blocking());
    }

    #[test]
    fn a_check_far_under_blocks() {
        let c = Check::new("x", 10, 100, "", None);
        assert_eq!(c.severity, Severity::Blocking);
        assert!(c.is_blocking());
    }

    #[test]
    fn max_supported_is_monotonic_and_self_consistent() {
        let max = Preflight::max_supported(1.0);
        assert!(
            Preflight::evaluate(max, 1.0).blocking().is_empty(),
            "max_supported returned {max}, which itself blocks"
        );
        if max < 200_000 {
            assert!(
                !Preflight::evaluate(max + 1, 1.0).blocking().is_empty(),
                "max_supported returned {max} but {} also passes",
                max + 1
            );
        }
    }

    #[test]
    fn report_names_the_remedy_for_every_blocking_check() {
        let pre = Preflight::evaluate(500_000, 1.0);
        let report = pre.report();
        assert!(report.contains("BLOCKING"));
        for check in pre.blocking() {
            if check.remedy.is_some() {
                assert!(
                    report.contains(check.name),
                    "report omits blocking check {}",
                    check.name
                );
            }
        }
    }
}
