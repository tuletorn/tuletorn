//! A fast, non-cryptographic hasher for the routing hot path.
//!
//! Hostname lookup happens once per request against a `HashMap` whose keys are
//! short, low-entropy, attacker-visible-but-not-attacker-chosen strings. The
//! std default (SipHash-1-3) costs roughly 1 ns/byte plus a fixed setup; for
//! `"api.example.com"` that dominates the actual radix-trie walk that follows.
//!
//! This is the FxHash construction used by rustc: multiply-and-rotate over
//! `usize`-sized words. It is not DoS-resistant, which is why route tables are
//! built only from the Kubernetes control plane (operator-supplied hostnames),
//! never from request data.

use std::hash::{BuildHasherDefault, Hasher};

/// 64-bit FxHash seed (fractional part of the golden ratio).
const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;
const ROTATE: u32 = 5;

/// `HashMap`/`HashSet` hasher builder. Use via [`FxHashMap`].
pub type FxBuildHasher = BuildHasherDefault<FxHasher>;

/// A `HashMap` keyed with [`FxHasher`].
pub type FxHashMap<K, V> = std::collections::HashMap<K, V, FxBuildHasher>;

/// Non-cryptographic multiply-rotate hasher.
///
/// Seeded with a non-zero constant and mixing the input length, so that the
/// all-zero inputs FxHash otherwise collapses together (`""`, `"\0"`, `"\0\0"`
/// all hash to 0 with a zero seed) stay distinct.
#[derive(Debug, Clone, Copy)]
pub struct FxHasher {
    hash: u64,
}

impl Default for FxHasher {
    #[inline]
    fn default() -> Self {
        Self { hash: SEED }
    }
}

impl FxHasher {
    #[inline]
    fn add_to_hash(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(ROTATE) ^ word).wrapping_mul(SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Length first: without it, `[0]` and `[0, 0]` fold to the same value
        // because both reduce to a single `add_to_hash(0)`.
        self.add_to_hash(bytes.len() as u64);
        let mut rest = bytes;
        // Eight bytes per multiply; the tail is folded in the same way so that
        // "abcdefgh" and "abcdefg" cannot collide trivially.
        while let Some((chunk, tail)) = rest.split_first_chunk::<8>() {
            self.add_to_hash(u64::from_ne_bytes(*chunk));
            rest = tail;
        }
        if let Some((chunk, tail)) = rest.split_first_chunk::<4>() {
            self.add_to_hash(u64::from(u32::from_ne_bytes(*chunk)));
            rest = tail;
        }
        if let Some((chunk, tail)) = rest.split_first_chunk::<2>() {
            self.add_to_hash(u64::from(u16::from_ne_bytes(*chunk)));
            rest = tail;
        }
        if let Some(&b) = rest.first() {
            self.add_to_hash(u64::from(b));
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add_to_hash(u64::from(i));
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add_to_hash(i);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add_to_hash(i as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn hash(bytes: &[u8]) -> u64 {
        let mut h = FxHasher::default();
        h.write(bytes);
        h.finish()
    }

    #[test]
    fn deterministic_and_length_sensitive() {
        assert_eq!(hash(b"api.example.com"), hash(b"api.example.com"));
        assert_ne!(hash(b"api.example.com"), hash(b"api.example.co"));
        assert_ne!(hash(b"abcdefgh"), hash(b"abcdefg"));
        assert_ne!(hash(b""), hash(b"\0"));
        assert_ne!(hash(b"\0"), hash(b"\0\0"));
        assert_ne!(hash(b""), 0, "an empty input must not hash to zero");
    }

    #[test]
    fn no_collisions_across_realistic_hostnames() {
        let mut seen = HashSet::new();
        for i in 0..5_000 {
            let host = format!("svc-{i}.namespace-{}.svc.cluster.local", i % 97);
            assert!(seen.insert(hash(host.as_bytes())), "collision at {host}");
        }
    }

    #[test]
    fn works_as_a_hashmap_hasher() {
        let mut m: FxHashMap<String, u32> = FxHashMap::default();
        for i in 0..1_000u32 {
            m.insert(format!("host-{i}.example.com"), i);
        }
        assert_eq!(m.len(), 1_000);
        assert_eq!(m.get("host-512.example.com"), Some(&512));
        assert_eq!(m.get("absent.example.com"), None);
    }
}
