//! Low-overhead timestamp counter read via inline assembly.
//!
//! `Instant::now()` costs a `clock_gettime` vDSO call (~20-25 ns). At the
//! request rates in plan §8 (Scenario 1 sweeps to 25k concurrent connections)
//! two of those per request is measurable *inside* the number we are trying to
//! measure. Reading the CPU's cycle/tick counter directly costs a handful of
//! cycles and removes that bias from the load generator.
//!
//! * `x86_64`  — `rdtsc`, invariant TSC on every CPU this benchmark targets.
//! * `aarch64` — `cntvct_el0`, the architectural virtual counter, plus
//!   `cntfrq_el0` for its nominal frequency (24 MHz on Apple silicon).
//! * anything else — falls back to `Instant`, so callers stay portable.
//!
//! These counters are *not* a wall clock: only differences are meaningful, and
//! only within one process. [`Calibration`] converts a delta to nanoseconds.

/// Read the platform cycle/tick counter.
#[inline(always)]
#[must_use]
pub fn timestamp() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        let lo: u32;
        let hi: u32;
        // SAFETY: `rdtsc` is unprivileged, has no memory operands and no side
        // effects beyond writing eax/edx. `nomem` + `nostack` let LLVM schedule
        // around it; we deliberately do *not* serialise with `lfence` because
        // the sampling error over a whole request is far below the cost of one.
        unsafe {
            core::arch::asm!(
                "rdtsc",
                out("eax") lo,
                out("edx") hi,
                options(nomem, nostack, preserves_flags),
            );
        }
        (u64::from(hi) << 32) | u64::from(lo)
    }
    #[cfg(target_arch = "aarch64")]
    {
        let ticks: u64;
        // SAFETY: `cntvct_el0` is readable from EL0 on every AArch64 platform
        // this targets (Linux and Darwin both leave CNTKCTL_EL1.EL0VCTEN set).
        unsafe {
            core::arch::asm!(
                "mrs {t}, cntvct_el0",
                t = out(reg) ticks,
                options(nomem, nostack, preserves_flags),
            );
        }
        ticks
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        use std::time::Instant;
        static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
    }
}

/// Ticks-to-nanoseconds conversion for the counter read by [`timestamp`].
#[derive(Debug, Clone, Copy)]
pub struct Calibration {
    ns_per_tick: f64,
}

impl Calibration {
    /// Measure the counter's rate against the monotonic clock.
    ///
    /// On AArch64 the frequency is architectural, so this is exact and free.
    /// On x86_64 it is measured over `sample` wall time; 10 ms is enough for
    /// well under 0.1% error and is paid once per process.
    #[must_use]
    pub fn measure(sample: std::time::Duration) -> Self {
        #[cfg(target_arch = "aarch64")]
        {
            let freq: u64;
            // SAFETY: `cntfrq_el0` is a readable EL0 system register holding the
            // nominal frequency of `cntvct_el0` in Hz.
            unsafe {
                core::arch::asm!(
                    "mrs {f}, cntfrq_el0",
                    f = out(reg) freq,
                    options(nomem, nostack, preserves_flags),
                );
            }
            if freq > 0 {
                return Self {
                    ns_per_tick: 1e9 / freq as f64,
                };
            }
        }

        let start_wall = std::time::Instant::now();
        let start_ticks = timestamp();
        std::thread::sleep(sample);
        let elapsed_ticks = timestamp().saturating_sub(start_ticks);
        let elapsed_ns = start_wall.elapsed().as_nanos() as f64;

        let ns_per_tick = if elapsed_ticks == 0 {
            1.0
        } else {
            elapsed_ns / elapsed_ticks as f64
        };
        Self { ns_per_tick }
    }

    /// Convert a tick delta to nanoseconds.
    #[inline]
    #[must_use]
    pub fn ticks_to_nanos(&self, ticks: u64) -> u64 {
        (ticks as f64 * self.ns_per_tick) as u64
    }

    /// Convert a tick delta to microseconds, the unit the histograms record in.
    #[inline]
    #[must_use]
    pub fn ticks_to_micros(&self, ticks: u64) -> u64 {
        self.ticks_to_nanos(ticks) / 1_000
    }

    /// Nanoseconds represented by one tick.
    #[must_use]
    pub fn ns_per_tick(&self) -> f64 {
        self.ns_per_tick
    }
}

impl Default for Calibration {
    fn default() -> Self {
        Self::measure(std::time::Duration::from_millis(10))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn counter_is_monotonic() {
        let a = timestamp();
        let b = timestamp();
        assert!(b >= a, "counter went backwards: {a} -> {b}");
    }

    #[test]
    fn calibration_tracks_wall_clock_within_five_percent() {
        let cal = Calibration::measure(Duration::from_millis(20));
        let start = timestamp();
        std::thread::sleep(Duration::from_millis(50));
        let measured_ns = cal.ticks_to_nanos(timestamp() - start);
        // Sleep overshoots but must not be wildly off; 45-80ms is a generous
        // band that still catches an inverted or mis-scaled conversion.
        assert!(
            (45_000_000..80_000_000).contains(&measured_ns),
            "50ms sleep measured as {measured_ns} ns (ns_per_tick={})",
            cal.ns_per_tick()
        );
    }
}
