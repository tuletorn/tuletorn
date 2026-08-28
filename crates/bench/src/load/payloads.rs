//! Standardised benchmark payloads (plan §8: 1 KB, 64 KB, 1 MB).

use bytes::Bytes;
use std::sync::OnceLock;

/// Build a deterministic payload of exactly `size_bytes`.
///
/// Content is JSON-shaped rather than a repeated single byte, so that a proxy
/// or upstream that compresses or de-duplicates cannot get an unrealistic win.
#[must_use]
pub fn generate_payload(size_bytes: usize) -> Bytes {
    const CHUNK: &[u8] = b"{\"key\":\"value\",\"data\":\"benchmark-payload-chunk-0123456789\"}\n";
    let mut data = Vec::with_capacity(size_bytes);
    while data.len() < size_bytes {
        let remaining = size_bytes - data.len();
        data.extend_from_slice(&CHUNK[..remaining.min(CHUNK.len())]);
    }
    Bytes::from(data)
}

/// The three sizes plan §8 sweeps over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSize {
    Small1Kb,
    Medium64Kb,
    Large1Mb,
}

impl PayloadSize {
    /// Byte count.
    #[must_use]
    pub const fn bytes(self) -> usize {
        match self {
            Self::Small1Kb => 1024,
            Self::Medium64Kb => 64 * 1024,
            Self::Large1Mb => 1024 * 1024,
        }
    }

    /// Short label used in reports and CLI flags.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Small1Kb => "1k",
            Self::Medium64Kb => "64k",
            Self::Large1Mb => "1m",
        }
    }

    /// Every size, in sweep order.
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [Self::Small1Kb, Self::Medium64Kb, Self::Large1Mb]
    }

    /// Parse a CLI label such as `1k`, `64k`, `1m`.
    #[must_use]
    pub fn parse(label: &str) -> Option<Self> {
        match label.trim().to_ascii_lowercase().as_str() {
            "1k" | "1kb" | "1024" => Some(Self::Small1Kb),
            "64k" | "64kb" => Some(Self::Medium64Kb),
            "1m" | "1mb" => Some(Self::Large1Mb),
            _ => None,
        }
    }
}

/// Lazily built, shared payload buffers.
///
/// `Bytes` is refcounted, so handing the same 1 MB payload to 10 000 concurrent
/// workers costs one allocation, not ten thousand.
pub struct StandardPayloads;

impl StandardPayloads {
    /// 1 KiB micro-API payload.
    #[must_use]
    pub fn small_1kb() -> Bytes {
        static P: OnceLock<Bytes> = OnceLock::new();
        P.get_or_init(|| generate_payload(PayloadSize::Small1Kb.bytes()))
            .clone()
    }

    /// 64 KiB document payload.
    #[must_use]
    pub fn medium_64kb() -> Bytes {
        static P: OnceLock<Bytes> = OnceLock::new();
        P.get_or_init(|| generate_payload(PayloadSize::Medium64Kb.bytes()))
            .clone()
    }

    /// 1 MiB streaming payload.
    #[must_use]
    pub fn large_1mb() -> Bytes {
        static P: OnceLock<Bytes> = OnceLock::new();
        P.get_or_init(|| generate_payload(PayloadSize::Large1Mb.bytes()))
            .clone()
    }

    /// Fetch by size.
    #[must_use]
    pub fn get(size: PayloadSize) -> Bytes {
        match size {
            PayloadSize::Small1Kb => Self::small_1kb(),
            PayloadSize::Medium64Kb => Self::medium_64kb(),
            PayloadSize::Large1Mb => Self::large_1mb(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_are_exactly_the_requested_size() {
        for size in [0, 1, 59, 60, 61, 1024, 64 * 1024, 1024 * 1024] {
            assert_eq!(generate_payload(size).len(), size, "size {size}");
        }
    }

    #[test]
    fn standard_sizes_match_the_plan() {
        assert_eq!(StandardPayloads::small_1kb().len(), 1024);
        assert_eq!(StandardPayloads::medium_64kb().len(), 65_536);
        assert_eq!(StandardPayloads::large_1mb().len(), 1_048_576);
    }

    #[test]
    fn cached_payloads_share_one_allocation() {
        let a = StandardPayloads::large_1mb();
        let b = StandardPayloads::large_1mb();
        assert_eq!(
            a.as_ptr(),
            b.as_ptr(),
            "payload must be shared, not rebuilt"
        );
    }

    #[test]
    fn labels_round_trip() {
        for size in PayloadSize::all() {
            assert_eq!(PayloadSize::parse(size.label()), Some(size));
        }
        assert_eq!(PayloadSize::parse("64KB"), Some(PayloadSize::Medium64Kb));
        assert_eq!(PayloadSize::parse("nonsense"), None);
    }

    #[test]
    fn generated_content_is_deterministic() {
        assert_eq!(generate_payload(4096), generate_payload(4096));
    }
}
