//! Node configuration.
//!
//! [`RaftConfig`] names a node, lists its peers, and sets the timing that drives
//! elections and heartbeats. Timing is expressed in **logical ticks**, not
//! wall-clock time: the core counts [`Event::Tick`](crate::Event::Tick)s, and
//! the caller decides how often to tick (every 10 ms, say). This is what keeps
//! the core free of any clock.
//!
//! The common case is one call — [`RaftConfig::new`] or [`RaftConfig::single`]
//! — with sensible defaults. The builder methods ([`election_timeout`],
//! [`heartbeat_interval`], [`seed`]) are there when you need to tune them.
//!
//! [`election_timeout`]: RaftConfig::election_timeout
//! [`heartbeat_interval`]: RaftConfig::heartbeat_interval
//! [`seed`]: RaftConfig::seed

use crate::types::NodeId;

/// Default lower bound of the randomised election timeout, in ticks.
const DEFAULT_ELECTION_MIN: u32 = 10;
/// Default upper bound of the randomised election timeout, in ticks.
const DEFAULT_ELECTION_MAX: u32 = 20;
/// Default heartbeat interval, in ticks. Must be well below the election
/// timeout so a healthy leader is never replaced.
const DEFAULT_HEARTBEAT: u32 = 3;
/// Default cap on entries carried by a single `AppendEntries`. Bounds message
/// size and per-RPC work so a far-behind follower is caught up in steady chunks
/// rather than one unbounded payload.
const DEFAULT_MAX_BATCH: usize = 64;

/// Configuration for a single [`RaftNode`](crate::RaftNode).
///
/// Build one with [`new`](RaftConfig::new) (or [`single`](RaftConfig::single)
/// for a one-node cluster) and optionally tune it with the builder methods,
/// which consume and return `self` so they chain.
///
/// # Examples
///
/// ```
/// use raft_io::RaftConfig;
///
/// // Node 1 in a three-node cluster, with tuned timing.
/// let cfg = RaftConfig::new(1, [2, 3])
///     .with_election_timeout(15, 30)
///     .with_heartbeat_interval(5);
/// assert_eq!(cfg.id(), 1);
/// assert_eq!(cfg.peers(), &[2, 3]);
/// ```
#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub(crate) id: NodeId,
    pub(crate) peers: Vec<NodeId>,
    pub(crate) election_timeout_min: u32,
    pub(crate) election_timeout_max: u32,
    pub(crate) heartbeat_interval: u32,
    pub(crate) max_batch: usize,
    pub(crate) seed: u64,
}

impl RaftConfig {
    /// Creates a configuration for node `id` whose peers are `peers`.
    ///
    /// `peers` is every *other* node in the cluster; do not include `id`. The
    /// quorum the node needs to win an election or commit an entry is derived
    /// from the total size (`peers.len() + 1`). Timing defaults to a `10..=20`
    /// tick election timeout and a `3` tick heartbeat, and the RNG seed defaults
    /// to `id` so distinct nodes jitter differently out of the box.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::RaftConfig;
    ///
    /// let cfg = RaftConfig::new(1, [2, 3, 4, 5]);
    /// assert_eq!(cfg.peers().len(), 4);
    /// ```
    #[must_use]
    pub fn new(id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Self {
        let peers = peers.into_iter().filter(|&p| p != id).collect();
        Self {
            id,
            peers,
            election_timeout_min: DEFAULT_ELECTION_MIN,
            election_timeout_max: DEFAULT_ELECTION_MAX,
            heartbeat_interval: DEFAULT_HEARTBEAT,
            max_batch: DEFAULT_MAX_BATCH,
            seed: id,
        }
    }

    /// Creates a configuration for a single-node cluster.
    ///
    /// A single node has no peers and a quorum of one, so it elects itself and
    /// commits its own proposals immediately. This is the trivial path for
    /// tests and local development.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::RaftConfig;
    ///
    /// let cfg = RaftConfig::single(1);
    /// assert!(cfg.peers().is_empty());
    /// ```
    #[must_use]
    pub fn single(id: NodeId) -> Self {
        Self::new(id, [])
    }

    /// Sets the randomised election timeout bounds, in ticks.
    ///
    /// A follower that hears nothing from a leader for a randomly chosen number
    /// of ticks in `[min, max]` starts an election. The spread is what breaks
    /// split votes. The bounds are normalised so `min >= 1` and `max >= min`,
    /// so out-of-order or zero arguments cannot wedge the node.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::RaftConfig;
    ///
    /// let cfg = RaftConfig::single(1).with_election_timeout(150, 300);
    /// assert_eq!(cfg.election_timeout(), (150, 300));
    ///
    /// // Arguments are normalised rather than rejected.
    /// let fixed = RaftConfig::single(1).with_election_timeout(0, 0);
    /// assert_eq!(fixed.election_timeout(), (1, 1));
    /// ```
    #[must_use]
    pub fn with_election_timeout(mut self, min: u32, max: u32) -> Self {
        let min = min.max(1);
        self.election_timeout_min = min;
        self.election_timeout_max = max.max(min);
        self
    }

    /// Sets the heartbeat interval, in ticks.
    ///
    /// A leader broadcasts a heartbeat every `interval` ticks to suppress
    /// elections. Keep it well below the election-timeout lower bound — a few
    /// times smaller is typical — so a single dropped heartbeat does not unseat
    /// a healthy leader. The value is normalised to at least `1`.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::RaftConfig;
    ///
    /// let cfg = RaftConfig::single(1).with_heartbeat_interval(5);
    /// assert_eq!(cfg.heartbeat_interval(), 5);
    /// ```
    #[must_use]
    pub fn with_heartbeat_interval(mut self, interval: u32) -> Self {
        self.heartbeat_interval = interval.max(1);
        self
    }

    /// Sets the maximum number of log entries a single `AppendEntries` carries.
    ///
    /// This bounds message size and the work done per replication RPC: a
    /// follower that has fallen far behind is caught up in batches of at most
    /// this many entries rather than in one unbounded payload. The value is
    /// normalised to at least `1` so replication can always make progress.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::RaftConfig;
    ///
    /// let cfg = RaftConfig::new(1, [2, 3]).with_max_batch(256);
    /// assert_eq!(cfg.max_batch(), 256);
    /// ```
    #[must_use]
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch.max(1);
        self
    }

    /// Sets the seed for the node's election-timeout RNG.
    ///
    /// Determinism is the point of the core, so the jitter source is seeded
    /// rather than drawn from the OS. Equal seeds reproduce equal timeout
    /// sequences; give peers distinct seeds (the default is the node id) so they
    /// do not jitter in lockstep.
    ///
    /// # Examples
    ///
    /// ```
    /// use raft_io::RaftConfig;
    ///
    /// let cfg = RaftConfig::single(1).with_seed(0xDEAD_BEEF);
    /// assert_eq!(cfg.seed(), 0xDEAD_BEEF);
    /// ```
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Returns this node's id.
    #[inline]
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Returns this node's peers (every other node in the cluster).
    #[inline]
    #[must_use]
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    /// Returns the election-timeout bounds as `(min, max)` ticks.
    #[inline]
    #[must_use]
    pub fn election_timeout(&self) -> (u32, u32) {
        (self.election_timeout_min, self.election_timeout_max)
    }

    /// Returns the heartbeat interval in ticks.
    #[inline]
    #[must_use]
    pub fn heartbeat_interval(&self) -> u32 {
        self.heartbeat_interval
    }

    /// Returns the maximum entries carried by a single `AppendEntries`.
    #[inline]
    #[must_use]
    pub fn max_batch(&self) -> usize {
        self.max_batch
    }

    /// Returns the election-timeout RNG seed.
    #[inline]
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_filters_self_from_peers() {
        let cfg = RaftConfig::new(1, [1, 2, 3]);
        assert_eq!(cfg.peers(), &[2, 3]);
    }

    #[test]
    fn test_defaults_are_applied() {
        let cfg = RaftConfig::new(2, [1]);
        assert_eq!(
            cfg.election_timeout(),
            (DEFAULT_ELECTION_MIN, DEFAULT_ELECTION_MAX)
        );
        assert_eq!(cfg.heartbeat_interval(), DEFAULT_HEARTBEAT);
        assert_eq!(cfg.max_batch(), DEFAULT_MAX_BATCH);
        assert_eq!(cfg.seed(), 2);
    }

    #[test]
    fn test_max_batch_is_at_least_one() {
        assert_eq!(RaftConfig::single(1).with_max_batch(0).max_batch(), 1);
        assert_eq!(RaftConfig::single(1).with_max_batch(128).max_batch(), 128);
    }

    #[test]
    fn test_single_has_no_peers() {
        assert!(RaftConfig::single(9).peers().is_empty());
    }

    #[test]
    fn test_election_timeout_normalises_bounds() {
        let cfg = RaftConfig::single(1).with_election_timeout(0, 0);
        assert_eq!(cfg.election_timeout(), (1, 1));

        let swapped = RaftConfig::single(1).with_election_timeout(30, 10);
        assert_eq!(swapped.election_timeout(), (30, 30));
    }

    #[test]
    fn test_heartbeat_interval_is_at_least_one() {
        assert_eq!(
            RaftConfig::single(1)
                .with_heartbeat_interval(0)
                .heartbeat_interval(),
            1
        );
    }

    #[test]
    fn test_builder_chains() {
        let cfg = RaftConfig::new(1, [2, 3])
            .with_election_timeout(15, 30)
            .with_heartbeat_interval(5)
            .with_seed(7);
        assert_eq!(cfg.election_timeout(), (15, 30));
        assert_eq!(cfg.heartbeat_interval(), 5);
        assert_eq!(cfg.seed(), 7);
    }
}
