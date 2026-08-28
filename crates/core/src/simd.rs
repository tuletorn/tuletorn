//! SIMD-accelerated byte primitives used on the request hot path.
//!
//! Plan §1/§3 call for SIMD UTF-8 validation, SIMD header matching and SIMD
//! delimiter search. This module is the single place those live, so every
//! data plane (Hyper, Pingora, Monoio) shares one implementation.
//!
//! Three tiers, picked at compile time and then at run time:
//!
//! 1. `aarch64` NEON via `core::arch::aarch64` — always available on AArch64,
//!    so it is selected statically with no runtime branch.
//! 2. `x86_64` AVX2, then SSE2 — AVX2 behind a cached `is_x86_feature_detected!`
//!    probe, SSE2 statically (it is in the x86_64 baseline).
//! 3. A portable scalar fallback that is still branch-light.
//!
//! Every vector routine has a scalar twin and a proptest-style differential
//! test below, because a silently wrong `eq_ignore_ascii_case` would corrupt
//! routing rather than crash it.

/// SIMD UTF-8 validation (`simdutf8`), ~4-11x faster than `core::str::from_utf8`
/// on non-ASCII input and equal on pure ASCII.
///
/// Returns `None` when `bytes` is not well-formed UTF-8.
#[inline]
#[must_use]
pub fn validate_utf8(bytes: &[u8]) -> Option<&str> {
    simdutf8::basic::from_utf8(bytes).ok()
}

/// SIMD single-byte search (`memchr`).
#[inline]
#[must_use]
pub fn find_byte(needle: u8, haystack: &[u8]) -> Option<usize> {
    memchr::memchr(needle, haystack)
}

/// Strip an optional `:port` suffix from a `Host` header value.
///
/// `"api.example.com:8443"` -> `"api.example.com"`. IPv6 literals
/// (`"[::1]:80"`) keep their brackets and lose only the port.
#[inline]
#[must_use]
pub fn host_without_port(host: &str) -> &str {
    let bytes = host.as_bytes();
    if bytes.first() == Some(&b'[') {
        // IPv6 literal: the port colon is the one after the closing bracket.
        return match find_byte(b']', bytes) {
            Some(close) => &host[..=close],
            None => host,
        };
    }
    match find_byte(b':', bytes) {
        Some(pos) => &host[..pos],
        None => host,
    }
}

// ---------------------------------------------------------------------------
// ASCII case-insensitive comparison
// ---------------------------------------------------------------------------

/// Vectorized ASCII-case-insensitive equality, used for `Host` and header-name
/// matching where HTTP semantics are case-insensitive but the wire bytes are not.
///
/// Equivalent to `a.eq_ignore_ascii_case(b)` but processes 16-32 bytes per step.
#[inline]
#[must_use]
pub fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    #[cfg(target_arch = "aarch64")]
    {
        // NEON is mandatory on AArch64: no runtime probe needed.
        unsafe { eq_ignore_ascii_case_neon(a, b) }
    }
    #[cfg(target_arch = "x86_64")]
    {
        if has_avx2() {
            // SAFETY: guarded by the cached AVX2 probe above.
            return unsafe { eq_ignore_ascii_case_avx2(a, b) };
        }
        // SAFETY: SSE2 is part of the x86_64 baseline.
        unsafe { eq_ignore_ascii_case_sse2(a, b) }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        eq_ignore_ascii_case_scalar(a, b)
    }
}

/// Portable reference implementation. Kept public for differential testing and
/// used verbatim on architectures without a vector path.
#[inline]
#[must_use]
pub fn eq_ignore_ascii_case_scalar(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Lowercase ASCII bytes into `dst`, vectorized. Returns the written slice.
///
/// Bytes outside `A-Z` pass through untouched, so this is safe on UTF-8
/// continuation bytes (all `>= 0x80`, none of which are in `A-Z`).
#[inline]
pub fn lowercase_ascii_into<'d>(src: &[u8], dst: &'d mut [u8]) -> Option<&'d mut [u8]> {
    if dst.len() < src.len() {
        return None;
    }
    let n = src.len();
    let dst = &mut dst[..n];
    dst.copy_from_slice(src);
    #[cfg(target_arch = "aarch64")]
    // SAFETY: NEON is mandatory on AArch64.
    unsafe {
        lowercase_ascii_neon(dst);
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: SSE2 is part of the x86_64 baseline.
    unsafe {
        lowercase_ascii_sse2(dst);
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    dst.make_ascii_lowercase();
    Some(dst)
}

// ---------------------------------------------------------------------------
// AArch64 / NEON
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn eq_ignore_ascii_case_neon(a: &[u8], b: &[u8]) -> bool {
    use core::arch::aarch64::*;

    let len = a.len();
    let (mut pa, mut pb) = (a.as_ptr(), b.as_ptr());
    let mut i = 0usize;

    // 'A' - 1 = 0x40 and 'Z' + 1 = 0x5B, compared as unsigned.
    // SAFETY: all loads below stay within `i + 16 <= len`.
    unsafe {
        let upper_lo = vdupq_n_u8(b'A');
        let upper_hi = vdupq_n_u8(b'Z');
        let case_bit = vdupq_n_u8(0x20);

        while i + 16 <= len {
            let va = vld1q_u8(pa);
            let vb = vld1q_u8(pb);

            // mask = (v >= 'A') & (v <= 'Z'), then OR in 0x20 to fold case.
            let a_is_upper = vandq_u8(vcgeq_u8(va, upper_lo), vcleq_u8(va, upper_hi));
            let b_is_upper = vandq_u8(vcgeq_u8(vb, upper_lo), vcleq_u8(vb, upper_hi));
            let la = vorrq_u8(va, vandq_u8(a_is_upper, case_bit));
            let lb = vorrq_u8(vb, vandq_u8(b_is_upper, case_bit));

            // Any differing lane makes the horizontal min zero.
            if vminvq_u8(vceqq_u8(la, lb)) != 0xFF {
                return false;
            }
            pa = pa.add(16);
            pb = pb.add(16);
            i += 16;
        }
    }

    eq_ignore_ascii_case_scalar(&a[i..], &b[i..])
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn lowercase_ascii_neon(buf: &mut [u8]) {
    use core::arch::aarch64::*;

    let len = buf.len();
    let mut p = buf.as_mut_ptr();
    let mut i = 0usize;

    // SAFETY: loads and stores stay within `i + 16 <= len`.
    unsafe {
        let upper_lo = vdupq_n_u8(b'A');
        let upper_hi = vdupq_n_u8(b'Z');
        let case_bit = vdupq_n_u8(0x20);
        while i + 16 <= len {
            let v = vld1q_u8(p);
            let is_upper = vandq_u8(vcgeq_u8(v, upper_lo), vcleq_u8(v, upper_hi));
            vst1q_u8(p, vorrq_u8(v, vandq_u8(is_upper, case_bit)));
            p = p.add(16);
            i += 16;
        }
    }
    buf[i..].make_ascii_lowercase();
}

// ---------------------------------------------------------------------------
// x86_64 / AVX2 + SSE2
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[inline]
fn has_avx2() -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    // 0 = unprobed, 1 = absent, 2 = present. Probing is idempotent, so a
    // benign race between threads is fine and no lock is needed.
    static CACHE: AtomicU8 = AtomicU8::new(0);
    match CACHE.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let present = is_x86_feature_detected!("avx2");
            CACHE.store(if present { 2 } else { 1 }, Ordering::Relaxed);
            present
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn eq_ignore_ascii_case_avx2(a: &[u8], b: &[u8]) -> bool {
    use core::arch::x86_64::*;

    let len = a.len();
    let (mut pa, mut pb) = (a.as_ptr(), b.as_ptr());
    let mut i = 0usize;

    // SAFETY: all loads stay within `i + 32 <= len`; loadu is unaligned-safe.
    unsafe {
        // Signed compares, so bias the range check by 0x80.
        let bias = _mm256_set1_epi8(-128i8);
        let lo = _mm256_set1_epi8((b'A' as i8).wrapping_sub(128).wrapping_sub(1));
        let hi = _mm256_set1_epi8((b'Z' as i8).wrapping_sub(128).wrapping_add(1));
        let case_bit = _mm256_set1_epi8(0x20);

        while i + 32 <= len {
            let va = _mm256_loadu_si256(pa.cast());
            let vb = _mm256_loadu_si256(pb.cast());

            let sa = _mm256_xor_si256(va, bias);
            let sb = _mm256_xor_si256(vb, bias);
            let a_up = _mm256_and_si256(_mm256_cmpgt_epi8(sa, lo), _mm256_cmpgt_epi8(hi, sa));
            let b_up = _mm256_and_si256(_mm256_cmpgt_epi8(sb, lo), _mm256_cmpgt_epi8(hi, sb));
            let la = _mm256_or_si256(va, _mm256_and_si256(a_up, case_bit));
            let lb = _mm256_or_si256(vb, _mm256_and_si256(b_up, case_bit));

            if _mm256_movemask_epi8(_mm256_cmpeq_epi8(la, lb)) != -1 {
                return false;
            }
            pa = pa.add(32);
            pb = pb.add(32);
            i += 32;
        }
    }

    eq_ignore_ascii_case_scalar(&a[i..], &b[i..])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn eq_ignore_ascii_case_sse2(a: &[u8], b: &[u8]) -> bool {
    use core::arch::x86_64::*;

    let len = a.len();
    let (mut pa, mut pb) = (a.as_ptr(), b.as_ptr());
    let mut i = 0usize;

    // SAFETY: all loads stay within `i + 16 <= len`; loadu is unaligned-safe.
    unsafe {
        let bias = _mm_set1_epi8(-128i8);
        let lo = _mm_set1_epi8((b'A' as i8).wrapping_sub(128).wrapping_sub(1));
        let hi = _mm_set1_epi8((b'Z' as i8).wrapping_sub(128).wrapping_add(1));
        let case_bit = _mm_set1_epi8(0x20);

        while i + 16 <= len {
            let va = _mm_loadu_si128(pa.cast());
            let vb = _mm_loadu_si128(pb.cast());

            let sa = _mm_xor_si128(va, bias);
            let sb = _mm_xor_si128(vb, bias);
            let a_up = _mm_and_si128(_mm_cmpgt_epi8(sa, lo), _mm_cmpgt_epi8(hi, sa));
            let b_up = _mm_and_si128(_mm_cmpgt_epi8(sb, lo), _mm_cmpgt_epi8(hi, sb));
            let la = _mm_or_si128(va, _mm_and_si128(a_up, case_bit));
            let lb = _mm_or_si128(vb, _mm_and_si128(b_up, case_bit));

            if _mm_movemask_epi8(_mm_cmpeq_epi8(la, lb)) != 0xFFFF {
                return false;
            }
            pa = pa.add(16);
            pb = pb.add(16);
            i += 16;
        }
    }

    eq_ignore_ascii_case_scalar(&a[i..], &b[i..])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn lowercase_ascii_sse2(buf: &mut [u8]) {
    use core::arch::x86_64::*;

    let len = buf.len();
    let mut p = buf.as_mut_ptr();
    let mut i = 0usize;

    // SAFETY: loads and stores stay within `i + 16 <= len`.
    unsafe {
        let bias = _mm_set1_epi8(-128i8);
        let lo = _mm_set1_epi8((b'A' as i8).wrapping_sub(128).wrapping_sub(1));
        let hi = _mm_set1_epi8((b'Z' as i8).wrapping_sub(128).wrapping_add(1));
        let case_bit = _mm_set1_epi8(0x20);
        while i + 16 <= len {
            let v = _mm_loadu_si128(p.cast());
            let s = _mm_xor_si128(v, bias);
            let is_up = _mm_and_si128(_mm_cmpgt_epi8(s, lo), _mm_cmpgt_epi8(hi, s));
            _mm_storeu_si128(p.cast(), _mm_or_si128(v, _mm_and_si128(is_up, case_bit)));
            p = p.add(16);
            i += 16;
        }
    }
    buf[i..].make_ascii_lowercase();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift so failures reproduce without a `rand` dev-dep.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn byte(&mut self) -> u8 {
            (self.next() >> 24) as u8
        }
    }

    #[test]
    fn utf8_validation_matches_std() {
        assert_eq!(
            validate_utf8(b"/api/v1/\xf0\x9f\x9a\x80"),
            Some("/api/v1/🚀")
        );
        assert!(validate_utf8(&[0xff, 0xfe]).is_none());
    }

    #[test]
    fn host_port_stripping() {
        assert_eq!(host_without_port("api.example.com:8443"), "api.example.com");
        assert_eq!(host_without_port("api.example.com"), "api.example.com");
        assert_eq!(host_without_port("[::1]:8080"), "[::1]");
        assert_eq!(host_without_port("[fe80::1]"), "[fe80::1]");
        assert_eq!(host_without_port(""), "");
    }

    #[test]
    fn eq_ignore_case_known_vectors() {
        assert!(eq_ignore_ascii_case(b"Host", b"host"));
        assert!(eq_ignore_ascii_case(b"X-Forwarded-For", b"x-forwarded-for"));
        assert!(!eq_ignore_ascii_case(b"host", b"hosts"));
        assert!(eq_ignore_ascii_case(b"", b""));
        // '@' (0x40) and '[' (0x5B) bracket A-Z; they must not be folded.
        assert!(!eq_ignore_ascii_case(b"@", b"`"));
        assert!(!eq_ignore_ascii_case(b"[", b"{"));
    }

    /// The vector paths must agree with the scalar reference on every length
    /// from 0 to 200, across all byte values including the A-Z boundaries.
    #[test]
    fn eq_ignore_case_differential_vs_scalar() {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for len in 0..200usize {
            for _ in 0..40 {
                let a: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
                // Half the trials compare against a case-flipped copy so the
                // "equal" branch is exercised, not just the early-out.
                let b: Vec<u8> = if rng.next().is_multiple_of(2) {
                    a.iter()
                        .map(|&c| if c.is_ascii_alphabetic() { c ^ 0x20 } else { c })
                        .collect()
                } else {
                    (0..len).map(|_| rng.byte()).collect()
                };
                assert_eq!(
                    eq_ignore_ascii_case(&a, &b),
                    eq_ignore_ascii_case_scalar(&a, &b),
                    "len={len} a={a:?} b={b:?}"
                );
            }
        }
    }

    #[test]
    fn lowercase_differential_vs_std() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for len in 0..200usize {
            let src: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
            let mut expected = src.clone();
            expected.make_ascii_lowercase();
            let mut dst = vec![0u8; len];
            let got = lowercase_ascii_into(&src, &mut dst).expect("dst is exactly len");
            assert_eq!(got, &expected[..], "len={len}");
        }
        // Too-small destination is reported, not truncated.
        let mut small = [0u8; 2];
        assert!(lowercase_ascii_into(b"abcdef", &mut small).is_none());
    }
}
