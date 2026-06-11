//! `FaultDisk` — a seeded, deterministic fault-injecting [`Disk`] ([STL-109]).
//!
//! The minimal [`Faults`](stele_storage::backend::Faults) schedule on
//! [`MemDisk`] lets a test make *one named operation* fail in FIFO order. This
//! is the richer model the testing strategy calls for
//! ([`docs/06-testing-strategy.md §5`](../../../docs/06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece),
//! [ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)): a
//! disk that wraps any inner [`Disk`] and, driven by a single seed, injects the
//! fault *classes* a real disk exhibits under power loss and hardware decay —
//!
//! | Class                     | Shape (what the model does)                                   |
//! |---------------------------|---------------------------------------------------------------|
//! | [`FullDisk`]              | `create`/`append` fail with [`StorageFull`]; nothing persists |
//! | [`TornWrite`]             | `append` persists a strict prefix, then errors — a torn tail  |
//! | [`ShortRead`]             | `read_at` returns *fewer* bytes than read (never a false EOF)  |
//! | [`BitFlip`]               | `read_at` flips one bit in the returned window — silent rot    |
//! | [`SlowSync`]              | `sync` succeeds but logs a latency in virtual ticks           |
//! | [`FailSync`]              | `sync` *fails* and persists nothing new — a failed fsync       |
//!
//! [`FullDisk`]: FaultKind::FullDisk
//! [`TornWrite`]: FaultKind::TornWrite
//! [`ShortRead`]: FaultKind::ShortRead
//! [`BitFlip`]: FaultKind::BitFlip
//! [`SlowSync`]: FaultKind::SlowSync
//! [`FailSync`]: FaultKind::FailSync
//! [`StorageFull`]: std::io::ErrorKind::StorageFull
//!
//! ## Determinism is the whole point
//!
//! Every fault decision is one draw from a seeded [`Rng`]. A run consumes the
//! stream in a fixed order, so **the same seed and the same [`FaultProfile`]
//! produce the exact same sequence of faults on every run** — the property the
//! seed-replay story rests on. Each injected fault is appended to a
//! seed-keyed [event log](FaultDisk::events), so a failing seed is inspectable,
//! not just reproducible. Classes can be toggled per test
//! ([`enable`](FaultDisk::enable) / [`disable`](FaultDisk::disable)), so a
//! recovery suite can isolate "torn writes only" from "everything".

use std::io;
use std::sync::{Arc, Mutex};

use stele_storage::backend::{Disk, DiskFile, FaultOp, MemDisk};

use crate::Rng;

/// Default ceiling for a [`SlowSync`](FaultKind::SlowSync) latency, in virtual
/// ticks — overridable with [`FaultProfile::with_max_slow_ticks`].
const DEFAULT_SLOW_TICKS: u64 = 16;

/// A class of injectable disk fault. Also the selector for the per-class
/// probability in a [`FaultProfile`] and the toggles on a [`FaultDisk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultKind {
    /// `read_at` flips one bit in the returned window — silent corruption a
    /// checksum must catch.
    BitFlip,
    /// `read_at` returns fewer bytes than were read, but never zero (a zero
    /// would masquerade as EOF) — a genuine short read.
    ShortRead,
    /// `append` persists a strict prefix of the bytes and then errors — the
    /// torn tail a crash mid-write leaves behind.
    TornWrite,
    /// `sync` still succeeds, but a latency (in virtual ticks) is recorded — a
    /// slow fsync that, under a scheduler, would reorder concurrent work.
    SlowSync,
    /// `sync` **fails** and durably persists nothing new — a failed fsync. The
    /// just-appended record's durability is then indeterminate, the case the WAL
    /// must treat as a crash and poison on ([STL-217]). Distinct from
    /// [`SlowSync`](FaultKind::SlowSync), which is slow but still durable.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    FailSync,
    /// `create`/`append` fail with [`StorageFull`](io::ErrorKind::StorageFull)
    /// and persist nothing — a full disk.
    FullDisk,
}

/// One injected fault, recorded in a [`FaultDisk`]'s seed-keyed event log.
///
/// `detail` carries the fault's *shape*: the persisted prefix length for a
/// [`TornWrite`](FaultKind::TornWrite), the returned length for a
/// [`ShortRead`](FaultKind::ShortRead), the flipped byte index for a
/// [`BitFlip`](FaultKind::BitFlip), the latency ticks for a
/// [`SlowSync`](FaultKind::SlowSync), and `0` for a
/// [`FullDisk`](FaultKind::FullDisk).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultEvent {
    /// Monotonic index within this disk — the fault's position in the sequence.
    pub seq: u64,
    /// The operation the fault fired on.
    pub op: FaultOp,
    /// Which class of fault fired.
    pub kind: FaultKind,
    /// The fault's shape (see the struct docs).
    pub detail: u64,
}

/// A per-class firing probability, resolved from an `f64` once at configuration
/// time into a `u64` threshold so the hot path is integer-only and identical
/// across platforms (no per-draw float math).
#[derive(Debug, Clone, Copy)]
enum Prob {
    /// Disabled — never fires, and draws no randomness.
    Never,
    /// Always fires (`p >= 1.0`).
    Always,
    /// Fires when the next `u64` draw is `< threshold` (`threshold` in `1..MAX`).
    Chance(u64),
}

impl Prob {
    /// Resolve a probability in `[0, 1]` to a threshold. `p <= 0` disables the
    /// class; `p >= 1` always fires; otherwise the threshold is `p` of the
    /// `u64` range (clamped to at least `1`, so a tiny `p` still fires).
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn from_prob(p: f64) -> Self {
        if p <= 0.0 {
            Self::Never
        } else if p >= 1.0 {
            Self::Always
        } else {
            Self::Chance(((p * (u64::MAX as f64)) as u64).max(1))
        }
    }

    /// Decide whether this class fires, consuming one `rng` draw when active so
    /// the stream advances identically whether or not a `Chance` lands.
    const fn fires(self, rng: &mut Rng) -> bool {
        match self {
            Self::Never => false,
            Self::Always => {
                let _ = rng.next_u64();
                true
            }
            Self::Chance(threshold) => rng.next_u64() < threshold,
        }
    }
}

/// The per-seed fault profile: a firing probability per [`FaultKind`] plus the
/// [`SlowSync`](FaultKind::SlowSync) latency ceiling.
///
/// Build one from [`none`](Self::none) and the `with_*` setters, e.g.
/// `FaultProfile::none().with_torn_write(0.01)` for "torn writes only, 1%".
#[derive(Debug, Clone)]
pub struct FaultProfile {
    bit_flip: Prob,
    short_read: Prob,
    torn_write: Prob,
    slow_sync: Prob,
    fail_sync: Prob,
    full_disk: Prob,
    max_slow_ticks: u64,
}

impl FaultProfile {
    /// A profile with every fault class disabled — the starting point for the
    /// `with_*` builders. A [`FaultDisk`] on this profile behaves exactly like
    /// its inner disk.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            bit_flip: Prob::Never,
            short_read: Prob::Never,
            torn_write: Prob::Never,
            slow_sync: Prob::Never,
            fail_sync: Prob::Never,
            full_disk: Prob::Never,
            max_slow_ticks: DEFAULT_SLOW_TICKS,
        }
    }

    /// The probability of `class`, by value (it is `Copy`).
    const fn prob(&self, class: FaultKind) -> Prob {
        match class {
            FaultKind::BitFlip => self.bit_flip,
            FaultKind::ShortRead => self.short_read,
            FaultKind::TornWrite => self.torn_write,
            FaultKind::SlowSync => self.slow_sync,
            FaultKind::FailSync => self.fail_sync,
            FaultKind::FullDisk => self.full_disk,
        }
    }

    const fn slot_mut(&mut self, class: FaultKind) -> &mut Prob {
        match class {
            FaultKind::BitFlip => &mut self.bit_flip,
            FaultKind::ShortRead => &mut self.short_read,
            FaultKind::TornWrite => &mut self.torn_write,
            FaultKind::SlowSync => &mut self.slow_sync,
            FaultKind::FailSync => &mut self.fail_sync,
            FaultKind::FullDisk => &mut self.full_disk,
        }
    }

    /// Set `class`'s firing probability (`p <= 0` disables, `p >= 1` always
    /// fires).
    pub fn set(&mut self, class: FaultKind, p: f64) {
        *self.slot_mut(class) = Prob::from_prob(p);
    }

    /// Disable `class` — equivalent to [`set`](Self::set) with `p == 0`.
    pub const fn disable(&mut self, class: FaultKind) {
        *self.slot_mut(class) = Prob::Never;
    }

    /// Builder form of [`set`](Self::set).
    #[must_use]
    pub fn with(mut self, class: FaultKind, p: f64) -> Self {
        self.set(class, p);
        self
    }

    /// Enable [`BitFlip`](FaultKind::BitFlip) with probability `p`.
    #[must_use]
    pub fn with_bit_flip(self, p: f64) -> Self {
        self.with(FaultKind::BitFlip, p)
    }

    /// Enable [`ShortRead`](FaultKind::ShortRead) with probability `p`.
    #[must_use]
    pub fn with_short_read(self, p: f64) -> Self {
        self.with(FaultKind::ShortRead, p)
    }

    /// Enable [`TornWrite`](FaultKind::TornWrite) with probability `p`.
    #[must_use]
    pub fn with_torn_write(self, p: f64) -> Self {
        self.with(FaultKind::TornWrite, p)
    }

    /// Enable [`SlowSync`](FaultKind::SlowSync) with probability `p`.
    #[must_use]
    pub fn with_slow_sync(self, p: f64) -> Self {
        self.with(FaultKind::SlowSync, p)
    }

    /// Enable [`FailSync`](FaultKind::FailSync) with probability `p`.
    #[must_use]
    pub fn with_fail_sync(self, p: f64) -> Self {
        self.with(FaultKind::FailSync, p)
    }

    /// Enable [`FullDisk`](FaultKind::FullDisk) with probability `p`.
    #[must_use]
    pub fn with_full_disk(self, p: f64) -> Self {
        self.with(FaultKind::FullDisk, p)
    }

    /// Set the [`SlowSync`](FaultKind::SlowSync) latency ceiling, in virtual
    /// ticks (clamped to at least `1` at fire time).
    #[must_use]
    pub const fn with_max_slow_ticks(mut self, ticks: u64) -> Self {
        self.max_slow_ticks = ticks;
        self
    }
}

impl Default for FaultProfile {
    fn default() -> Self {
        Self::none()
    }
}

/// The mutable, shared core of a [`FaultDisk`] — the seeded RNG, the live
/// profile, and the event log. Shared (behind a `Mutex`) by the disk and every
/// open [`FaultFile`], so the whole disk draws from one deterministic stream.
#[derive(Debug)]
struct FaultState {
    rng: Rng,
    profile: FaultProfile,
    log: Vec<FaultEvent>,
    next_seq: u64,
}

impl FaultState {
    /// Decide whether `class` fires now, drawing from the shared stream.
    const fn fires(&mut self, class: FaultKind) -> bool {
        self.profile.prob(class).fires(&mut self.rng)
    }

    /// Append a fired fault to the seed-keyed log.
    fn record(&mut self, op: FaultOp, kind: FaultKind, detail: u64) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.log.push(FaultEvent {
            seq,
            op,
            kind,
            detail,
        });
    }

    /// Decide a `create`: `true` (and log it) iff the disk is full.
    fn plan_create(&mut self) -> bool {
        let full = self.fires(FaultKind::FullDisk);
        if full {
            self.record(FaultOp::Create, FaultKind::FullDisk, 0);
        }
        full
    }

    /// Decide an `append` of `len` bytes — full disk, torn prefix, or clean.
    fn plan_append(&mut self, len: usize) -> Append {
        if self.fires(FaultKind::FullDisk) {
            self.record(FaultOp::Append, FaultKind::FullDisk, 0);
            Append::FullDisk
        } else if self.fires(FaultKind::TornWrite) {
            // A strict prefix in `0..len`, so the write always loses bytes.
            let prefix = self.rng.below_usize(len);
            self.record(FaultOp::Append, FaultKind::TornWrite, prefix as u64);
            Append::Torn(prefix)
        } else {
            Append::Clean
        }
    }

    /// Decide a `read_at` that returned `n` bytes into `buf`: apply a short read
    /// and/or a bit flip, and return the (possibly shortened) length.
    fn plan_read(&mut self, n: usize, buf: &mut [u8]) -> usize {
        let mut len = n;
        // Short read: strictly fewer bytes, but at least one — never a zero that
        // a caller would read as EOF.
        if self.fires(FaultKind::ShortRead) && n >= 2 {
            len = 1 + self.rng.below_usize(n - 1);
            self.record(FaultOp::ReadAt, FaultKind::ShortRead, len as u64);
        }
        // Bit flip within the (possibly shortened) returned window.
        if self.fires(FaultKind::BitFlip) && len > 0 {
            let idx = self.rng.below_usize(len);
            let bit = self.rng.below_usize(8);
            buf[idx] ^= 1u8 << bit;
            self.record(FaultOp::ReadAt, FaultKind::BitFlip, idx as u64);
        }
        len
    }

    /// Decide a `sync`: a failed fsync ([`FailSync`](FaultKind::FailSync)) takes
    /// precedence — it persists nothing and errors — otherwise a slow fsync
    /// ([`SlowSync`](FaultKind::SlowSync)) logs a latency but still succeeds.
    ///
    /// Drawing `FailSync` first keeps the stream stable for the common
    /// `FailSync`-disabled profiles: a [`Prob::Never`] class draws no randomness,
    /// so the subsequent `SlowSync` draw lands exactly where it always did.
    fn plan_sync(&mut self) -> SyncPlan {
        if self.fires(FaultKind::FailSync) {
            self.record(FaultOp::Sync, FaultKind::FailSync, 0);
            return SyncPlan::Fail;
        }
        if self.fires(FaultKind::SlowSync) {
            let max = self.profile.max_slow_ticks.max(1);
            let ticks = 1 + self.rng.below(max);
            self.record(FaultOp::Sync, FaultKind::SlowSync, ticks);
        }
        SyncPlan::Clean
    }
}

/// What a `sync` should do, decided under the state lock and then carried out
/// without it.
enum SyncPlan {
    /// The fsync fails and persists nothing new — a failed fsync.
    Fail,
    /// The fsync proceeds (possibly after a recorded `SlowSync` latency).
    Clean,
}

/// A seeded, deterministic fault-injecting [`Disk`].
///
/// Wraps an inner backend — a fresh [`MemDisk`] by default, or another via
/// [`with_inner`](Self::with_inner). See the module docs for the fault model.
#[derive(Debug, Clone)]
pub struct FaultDisk<D = MemDisk> {
    inner: D,
    seed: u64,
    state: Arc<Mutex<FaultState>>,
}

impl FaultDisk<MemDisk> {
    /// A fault-injecting disk over a fresh [`MemDisk`], driven by `seed` and
    /// `profile`.
    #[must_use]
    pub fn new(seed: u64, profile: FaultProfile) -> Self {
        Self::with_inner(MemDisk::new(), seed, profile)
    }
}

impl<D> FaultDisk<D> {
    /// A fault-injecting disk over an existing `inner` backend.
    #[must_use]
    pub fn with_inner(inner: D, seed: u64, profile: FaultProfile) -> Self {
        Self {
            inner,
            seed,
            state: Arc::new(Mutex::new(FaultState {
                rng: Rng::new(seed),
                profile,
                log: Vec::new(),
                next_seq: 0,
            })),
        }
    }

    /// The seed driving this disk — the key its [event log](Self::events) is
    /// reproducible under.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// A snapshot of the faults injected so far, in fire order.
    #[must_use]
    pub fn events(&self) -> Vec<FaultEvent> {
        self.state.lock().unwrap().log.clone()
    }

    /// How many faults have fired so far.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.state.lock().unwrap().log.len()
    }

    /// Enable (or re-weight) `class` at probability `p` for subsequent
    /// operations — a per-test toggle.
    pub fn enable(&self, class: FaultKind, p: f64) {
        self.state.lock().unwrap().profile.set(class, p);
    }

    /// Disable `class` for subsequent operations — a per-test toggle.
    pub fn disable(&self, class: FaultKind) {
        self.state.lock().unwrap().profile.disable(class);
    }
}

/// A [`StorageFull`](io::ErrorKind::StorageFull) error standing in for a full
/// disk.
fn full_disk_error() -> io::Error {
    io::Error::new(io::ErrorKind::StorageFull, "stele-sim: simulated full disk")
}

/// The error a failed fsync ([`FaultKind::FailSync`]) reports.
fn fail_sync_error() -> io::Error {
    io::Error::other("stele-sim: simulated fsync failure")
}

impl<D: Disk> Disk for FaultDisk<D> {
    type File = FaultFile<D::File>;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        // Surface the inner backend's contract errors (InvalidInput for a
        // non-flat name, AlreadyExists for a duplicate) *before* the full-disk
        // injection, so a fault can never mask — or spuriously log against — a
        // call the `Disk` contract says must fail for another reason.
        let inner = self.inner.create(name)?;
        if self.state.lock().unwrap().plan_create() {
            // A full disk fails the create: close the handle and undo the file
            // the inner backend just made, so nothing is persisted (the
            // FullDisk contract). If the cleanup itself fails we surface *that*
            // error rather than falsely claiming StorageFull over a file that
            // still lingers.
            drop(inner);
            self.inner.remove(name)?;
            return Err(full_disk_error());
        }
        Ok(FaultFile {
            inner,
            state: Arc::clone(&self.state),
        })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let inner = self.inner.open(name)?;
        Ok(FaultFile {
            inner,
            state: Arc::clone(&self.state),
        })
    }

    fn list(&self) -> io::Result<Vec<String>> {
        self.inner.list()
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        self.inner.remove(name)
    }
}

/// What an `append` should do, decided under the state lock and then carried
/// out without it.
enum Append {
    /// Persist nothing, fail full.
    FullDisk,
    /// Persist `bytes[..len]`, then fail torn.
    Torn(usize),
    /// Persist everything.
    Clean,
}

/// A single file within a [`FaultDisk`]. Shares the disk's seeded fault state,
/// so reads and writes through it draw from the same deterministic stream.
#[derive(Debug)]
pub struct FaultFile<F> {
    inner: F,
    state: Arc<Mutex<FaultState>>,
}

impl<F: DiskFile> DiskFile for FaultFile<F> {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        // An empty append can be neither torn nor space-consuming — delegate.
        if bytes.is_empty() {
            return self.inner.append(bytes);
        }
        let plan = self.state.lock().unwrap().plan_append(bytes.len());
        match plan {
            Append::FullDisk => Err(full_disk_error()),
            Append::Torn(len) => {
                self.inner.append(&bytes[..len])?;
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "stele-sim: torn write",
                ))
            }
            Append::Clean => self.inner.append(bytes),
        }
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read_at(offset, buf)?;
        if n == 0 {
            return Ok(0);
        }
        Ok(self.state.lock().unwrap().plan_read(n, buf))
    }

    fn sync(&mut self) -> io::Result<()> {
        // Resolve the plan and drop the state lock *before* touching the inner disk.
        let plan = self.state.lock().unwrap().plan_sync();
        match plan {
            // A failed fsync persists nothing new and errors — the caller (the WAL)
            // must treat it as a crash ([STL-217]).
            SyncPlan::Fail => Err(fail_sync_error()),
            // A clean (possibly slow) fsync still durably persists — slow, not lost.
            SyncPlan::Clean => self.inner.sync(),
        }
    }

    fn len(&self) -> u64 {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a fixed workload and return the disk's event log — the helper the
    /// determinism tests replay.
    fn run(seed: u64, profile: FaultProfile, appends: usize) -> Vec<FaultEvent> {
        let disk = FaultDisk::new(seed, profile);
        let mut file = None;
        for _ in 0..64 {
            // `create` can hit a full-disk fault; retry a bounded number of times.
            if let Ok(f) = disk.create("wal") {
                file = Some(f);
                break;
            }
        }
        let mut file = file.expect("create landed within the retry budget");
        let mut rng = Rng::new(seed);
        for _ in 0..appends {
            let payload_len = 1 + rng.below_usize(16);
            let payload = rng.bytes(payload_len);
            let _ = file.append(&payload);
            if rng.below(4) == 0 {
                let _ = file.sync();
            }
            let buf_len = 1 + rng.below_usize(24);
            let mut buf = vec![0u8; buf_len];
            let _ = file.read_at(0, &mut buf);
        }
        disk.events()
    }

    #[test]
    fn torn_write_seed_42_is_reproducible() {
        // The ticket's DoD, verbatim: seed 42, torn-write probability 0.01,
        // identical fault sequence on every run.
        let profile = || FaultProfile::none().with_torn_write(0.01);
        let a = run(42, profile(), 2_000);
        let b = run(42, profile(), 2_000);
        assert_eq!(
            a, b,
            "seed 42 must replay the exact same torn-write sequence"
        );
        assert!(
            a.iter().all(|e| e.kind == FaultKind::TornWrite),
            "only torn writes are enabled",
        );
        assert!(
            !a.is_empty(),
            "2000 appends at p=0.01 must trip at least one torn write",
        );
    }

    #[test]
    fn distinct_seeds_diverge() {
        let profile = || FaultProfile::none().with_torn_write(0.05);
        assert_ne!(
            run(1, profile(), 2_000),
            run(2, profile(), 2_000),
            "different seeds must produce different fault sequences",
        );
    }

    #[test]
    fn full_disk_fails_operations_and_persists_nothing() {
        // Start clean so a file exists, then turn the disk full mid-run.
        let disk = FaultDisk::new(0, FaultProfile::none());
        let mut file = disk.create("x").expect("create on a clean disk");
        file.append(b"keep").expect("clean append");
        disk.enable(FaultKind::FullDisk, 1.0);

        let err = file
            .append(b"lost")
            .expect_err("append on a full disk fails");
        assert_eq!(err.kind(), io::ErrorKind::StorageFull);
        assert_eq!(file.len(), 4, "a full-disk append persists nothing");
        assert_eq!(
            disk.create("y")
                .expect_err("create on a full disk fails")
                .kind(),
            io::ErrorKind::StorageFull,
        );
        // The failed create left nothing behind — only the pre-fault "x" remains.
        assert_eq!(disk.list().expect("list"), vec!["x".to_string()]);
    }

    #[test]
    fn create_contract_errors_take_precedence_over_full_disk() {
        // Even with the full-disk fault always armed, a non-flat name is an
        // InvalidInput (the `Disk` contract), not a StorageFull, and logs no
        // fault — the inner backend rejects it before the injection runs.
        let disk = FaultDisk::new(0, FaultProfile::none().with_full_disk(1.0));
        assert_eq!(
            disk.create("a/b").expect_err("non-flat name").kind(),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(disk.event_count(), 0, "a rejected name records no fault");

        // A duplicate name is AlreadyExists, again ahead of the full-disk fault.
        let disk = FaultDisk::new(0, FaultProfile::none());
        disk.create("x").expect("first create");
        disk.enable(FaultKind::FullDisk, 1.0);
        assert_eq!(
            disk.create("x").expect_err("duplicate name").kind(),
            io::ErrorKind::AlreadyExists,
        );
        assert_eq!(disk.event_count(), 0, "a duplicate records no fault");
    }

    #[test]
    fn torn_write_persists_a_strict_prefix() {
        let disk = FaultDisk::new(7, FaultProfile::none().with_torn_write(1.0));
        let mut file = disk.create("x").expect("create");
        let err = file.append(b"abcdefghij").expect_err("torn write errors");
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
        // The persisted length is the recorded prefix, strictly less than 10.
        assert_eq!(file.len(), disk.events()[0].detail);
        assert!(file.len() < 10, "a torn write must lose bytes");
    }

    #[test]
    fn short_read_returns_fewer_but_nonzero() {
        let disk = FaultDisk::new(3, FaultProfile::none().with_short_read(1.0));
        let mut file = disk.create("x").expect("create");
        file.append(b"0123456789").expect("clean append");
        let mut buf = [0u8; 10];
        let n = file.read_at(0, &mut buf).expect("read");
        assert!((1..10).contains(&n), "short read is fewer but not EOF: {n}");
    }

    #[test]
    fn bit_flip_corrupts_one_byte() {
        let disk = FaultDisk::new(5, FaultProfile::none().with_bit_flip(1.0));
        let mut file = disk.create("x").expect("create");
        file.append(b"AAAAAAAA").expect("clean append");
        let mut buf = [0u8; 8];
        let n = file.read_at(0, &mut buf).expect("read");
        assert_eq!(n, 8);
        let flipped = buf[..n].iter().filter(|&&b| b != b'A').count();
        assert_eq!(flipped, 1, "exactly one byte is corrupted");
    }

    #[test]
    fn slow_sync_succeeds_and_logs_latency() {
        let disk = FaultDisk::new(9, FaultProfile::none().with_slow_sync(1.0));
        let mut file = disk.create("x").expect("create");
        file.sync().expect("a slow sync still succeeds");
        let events = disk.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, FaultKind::SlowSync);
        assert!(events[0].detail >= 1, "latency is at least one tick");
    }

    #[test]
    fn fail_sync_errors_and_records_the_fault() {
        // A failed fsync errors (it does not call the inner sync) and logs a
        // `FailSync` event — the path STL-217's WAL poison rests on.
        let disk = FaultDisk::new(13, FaultProfile::none().with_fail_sync(1.0));
        let mut file = disk.create("x").expect("create");
        let err = file.sync().expect_err("a failed fsync errors");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        let events = disk.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, FaultKind::FailSync);
        assert_eq!(events[0].op, FaultOp::Sync);
    }

    #[test]
    fn fail_sync_seed_is_reproducible() {
        // Same seed + profile ⇒ identical fail-sync sequence, the seed-replay
        // property the fault sweep rests on.
        let profile = || FaultProfile::none().with_fail_sync(0.25);
        let a = run(99, profile(), 2_000);
        let b = run(99, profile(), 2_000);
        assert_eq!(a, b, "seed 99 must replay the same fail-sync sequence");
        assert!(
            a.iter().all(|e| e.kind == FaultKind::FailSync),
            "only failed fsyncs are enabled",
        );
        assert!(
            !a.is_empty(),
            "2000 ops at p=0.25 must trip at least one failed fsync",
        );
    }

    #[test]
    fn disable_stops_a_class_mid_run() {
        let disk = FaultDisk::new(11, FaultProfile::none().with_torn_write(1.0));
        let mut file = disk.create("x").expect("create");
        file.append(b"hello").expect_err("torn while enabled");
        disk.disable(FaultKind::TornWrite);
        file.append(b"world").expect("clean once disabled");
        assert_eq!(disk.event_count(), 1, "only the pre-disable fault fired");
    }

    #[test]
    fn none_profile_behaves_like_the_inner_disk() {
        let disk = FaultDisk::new(0, FaultProfile::none());
        let mut file = disk.create("x").expect("create");
        file.append(b"payload").expect("append");
        let mut buf = [0u8; 7];
        let n = file.read_at(0, &mut buf).expect("read");
        assert_eq!(&buf[..n], b"payload");
        file.sync().expect("sync");
        assert_eq!(disk.event_count(), 0, "no faults on an empty profile");
    }
}
