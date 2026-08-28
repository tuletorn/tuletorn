//! Allocator selection for high-concurrency data planes.
//!
//! # Why there is no `#[global_allocator]` in this crate
//!
//! A `#[global_allocator]` declared in a *library* behind a Cargo feature is a
//! reproducibility hazard: Cargo unifies features across every package in a
//! single build invocation, so `cargo build --workspace` and
//! `cargo build -p lb-proxy-hyper` could select *different* allocators for the
//! very same binary. For a benchmark whose stated goal (plan §8, Scenario 2) is
//! comparing allocator behaviour against the Go GC, that is fatal.
//!
//! Each binary therefore declares its own `#[global_allocator]` in its own
//! `main.rs` against its own direct dependency, and calls [`register`] so that
//! the running process can report which allocator is actually linked in.

use std::sync::OnceLock;

static ALLOCATOR_NAME: OnceLock<&'static str> = OnceLock::new();

/// Record the allocator linked into this binary. Call once from `main`.
///
/// Subsequent calls are ignored, so the first registration wins.
pub fn register(name: &'static str) {
    let _ = ALLOCATOR_NAME.set(name);
}

/// Name of the allocator registered by this binary, or `"system"` if none.
#[must_use]
pub fn allocator_name() -> &'static str {
    ALLOCATOR_NAME.get().copied().unwrap_or("system")
}

#[cfg(test)]
mod tests {
    #[test]
    fn defaults_to_system_then_sticks() {
        assert_eq!(super::allocator_name(), "system");
        super::register("jemalloc");
        assert_eq!(super::allocator_name(), "jemalloc");
        super::register("mimalloc");
        assert_eq!(
            super::allocator_name(),
            "jemalloc",
            "first registration wins"
        );
    }
}
