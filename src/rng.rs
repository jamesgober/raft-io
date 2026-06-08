//! A tiny deterministic pseudo-random generator for election timeouts.
//!
//! Raft randomises each node's election timeout so that nodes that time out
//! together do not split the vote forever. The protocol core, however, must be
//! deterministic — given the same seed and the same events it must always
//! behave identically, or property tests could not reproduce failures and a
//! replayed log could not be trusted. The standard library's thread RNG draws
//! from the OS, which is neither.
//!
//! [`Rng`] resolves the tension: it is the [SplitMix64] generator, seeded
//! explicitly from [`RaftConfig`](crate::RaftConfig), so randomness is real but
//! reproducible. SplitMix64 is a single multiply-xor-shift sequence — a handful
//! of instructions, no allocation, good enough statistical quality for jittered
//! timeouts (it is not, and is not used as, a cryptographic generator).
//!
//! [SplitMix64]: https://prng.di.unimi.it/splitmix64.c

/// A seedable SplitMix64 pseudo-random generator.
#[derive(Clone, Debug)]
pub(crate) struct Rng {
    state: u64,
}

impl Rng {
    /// Creates a generator from `seed`. Equal seeds yield equal streams.
    #[inline]
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next 64-bit value and advances the state.
    #[inline]
    pub(crate) fn next_u64(&mut self) -> u64 {
        // SplitMix64: advance by the golden-ratio increment, then avalanche.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a value uniformly in the inclusive range `[min, max]`.
    ///
    /// If `max <= min` the function returns `min`, so a degenerate range is
    /// safe rather than a panic. The modulo bias across the small spans used for
    /// timeouts is negligible.
    #[inline]
    pub(crate) fn gen_range(&mut self, min: u32, max: u32) -> u32 {
        if max <= min {
            return min;
        }
        let span = u64::from(max - min) + 1;
        min + (self.next_u64() % span) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_seed_yields_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..32 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn test_different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn test_gen_range_stays_within_bounds() {
        let mut r = Rng::new(7);
        for _ in 0..10_000 {
            let v = r.gen_range(150, 300);
            assert!((150..=300).contains(&v));
        }
    }

    #[test]
    fn test_gen_range_degenerate_returns_min() {
        let mut r = Rng::new(7);
        assert_eq!(r.gen_range(10, 10), 10);
        assert_eq!(r.gen_range(10, 5), 10);
    }

    #[test]
    fn test_gen_range_covers_both_endpoints() {
        let mut r = Rng::new(99);
        let mut seen_min = false;
        let mut seen_max = false;
        for _ in 0..10_000 {
            match r.gen_range(0, 3) {
                0 => seen_min = true,
                3 => seen_max = true,
                _ => {}
            }
        }
        assert!(seen_min && seen_max);
    }
}
