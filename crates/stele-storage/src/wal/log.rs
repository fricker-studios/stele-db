//! The WAL: append, group-commit fsync, segment rotation.
//!
//! See [`crate::wal`] for the durability contract.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use stele_common::metrics::SharedMetrics;

use super::record::{HEADER_LEN, MAX_PAYLOAD_LEN, encode};
use super::segment;
use crate::backend::{Disk, DiskFile};

/// Tuning knobs.
#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    /// Rotate to a new segment once the *current* segment's on-disk size has
    /// reached this many bytes. The check is "before write": a record is never
    /// split across segments.
    pub segment_size_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        // 64 MiB matches Postgres's default WAL segment size — small enough that
        // recovery scans are cheap, large enough that rotation cost is amortized.
        Self {
            segment_size_bytes: 64 * 1024 * 1024,
        }
    }
}

/// A position in the log: `(segment index, byte offset within that segment)`.
///
/// Offsets are *post-record* — i.e. `LogOffset` returned from `append` points at
/// the byte immediately following the just-staged record's last byte. This makes
/// `LogOffset` directly usable as a [`Checkpoint`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogOffset {
    pub segment_index: u64,
    pub byte_offset: u64,
}

impl LogOffset {
    pub const ZERO: Self = Self {
        segment_index: 0,
        byte_offset: 0,
    };
}

/// Where replay should resume from. Construct from a [`LogOffset`] returned by
/// [`Wal::append`], or from [`Wal::durable_end`] for a persisted resume point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint(pub LogOffset);

impl Checkpoint {
    pub const BEGIN: Self = Self(LogOffset::ZERO);
}

/// Errors surfaced from the WAL.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("payload too large: {0} > {MAX_PAYLOAD_LEN}")]
    PayloadTooLarge(usize),

    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// The WAL is **poisoned**: a prior durability hazard left the log in a state
    /// no further write may build on, so every subsequent [`append`](Wal::append)
    /// / [`tick`](Wal::tick) refuses with this error and the engine must stop and
    /// restart into recovery, which opens a fresh, unpoisoned WAL. Two causes,
    /// both crashes rather than clean aborts under the WAL contract (invariant 2):
    ///
    /// * a **failed fsync** ([`tick`](Wal::tick) or the segment-boundary sync in
    ///   rotation) leaves the staged record's durability indeterminate — poison so
    ///   a later successful `tick` can never flush it under the guise of an aborted
    ///   op ([STL-217]); and
    /// * a **torn append** — bytes physically landed past the staged end yet
    ///   [`append`](Wal::append) returned `Err` — leaves stray bytes a later append
    ///   would build past, desyncing the WAL's offset bookkeeping from the file and
    ///   shearing recovery at the torn frame. Poison so the garbage is never built
    ///   on ([STL-299]). A *clean* append failure (nothing landed) is not a crash
    ///   and does not poison.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    /// [STL-299]: https://allegromusic.atlassian.net/browse/STL-299
    #[error("WAL is poisoned: a prior fsync failed or an append tore; the engine must recover")]
    Poisoned,
}

/// The WAL handle. Cheap to clone — internal state is shared.
pub struct Wal<D: Disk> {
    inner: Arc<Mutex<Inner<D>>>,
}

impl<D: Disk> Clone for Wal<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub(super) struct Inner<D: Disk> {
    disk: D,
    config: WalConfig,
    /// Currently-open segment for appends.
    current_segment_index: u64,
    current: D::File,
    /// Bytes staged but not yet `sync`'d. The current segment file has already
    /// received them (so `read_at` would see them); they just aren't durable.
    staged_end: LogOffset,
    /// The most-recent `LogOffset` that is *durable*.
    durable_end: LogOffset,
    /// Pending commit waiters. Order is insertion order, but a drain removes
    /// every waiter whose target is now durable (the *order* of waking is
    /// unspecified — `swap_remove` is used internally).
    waiters: Vec<Waiter>,
    /// Set once a durability hazard makes further writes unsafe: an fsync fails
    /// inside [`drain_tick`] or [`rotate`] (the staged record's durability is then
    /// indeterminate, [STL-217]), or an [`append`](Wal::append) *tears* — bytes
    /// land past `staged_end` yet the call fails ([STL-299]). Either way the WAL
    /// refuses every further [`append`](Wal::append) / [`tick`](Wal::tick)
    /// ([`WalError::Poisoned`]) so a later op can neither flush the staged record
    /// under the guise of an aborted op nor build past the torn frame. Reads
    /// (replay) stay available so recovery can run; recovery opens a fresh WAL,
    /// which starts unpoisoned.
    poisoned: bool,
    /// The session's shared metric registry, when one has been installed
    /// ([`Wal::set_metrics`], [STL-253]): appends and fsyncs report into it.
    /// Pure atomic bumps — no time is read unless the *host* installed a time
    /// source on the registry, so the deterministic core stays clock-free
    /// ([ADR-0010]).
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    /// [ADR-0010]: ../../../../docs/adr/0010-deterministic-simulation-testing.md
    metrics: Option<SharedMetrics>,
}

/// One distinct `LogOffset` target shared by 1..N pending `Commit` futures.
///
/// Multiple `commit()` futures may legitimately await the same offset (e.g. a
/// transaction that retries its commit, or two consumers waiting on the same
/// boundary). Each contributing future registers its own waker; on drain we
/// wake all of them.
struct Waiter {
    target: LogOffset,
    wakers: Vec<Waker>,
}

impl<D: Disk> Wal<D> {
    /// Open the WAL backed by `disk`. If `disk` already contains WAL segments,
    /// the highest-numbered segment is reopened for append. If empty, segment 0
    /// is created.
    ///
    /// Note: this constructor does *not* validate existing record contents —
    /// callers needing to verify the log should use [`Wal::replay_from`] first.
    pub fn open(disk: D, config: WalConfig) -> Result<Self, WalError> {
        let segments = list_segments(&disk)?;
        let (current_segment_index, current) = if let Some(idx) = segments.last().copied() {
            (idx, disk.open(&segment::name_for(idx))?)
        } else {
            (0, disk.create(&segment::name_for(0))?)
        };
        // Directory fence ([STL-232]), on *both* paths: recovery rediscovers
        // the log by listing the disk, so every segment's *entry* must be
        // durable before a record fsync'd into it can count as durable.
        // Fencing unconditionally (not just after the create) also heals any
        // entry a previous incarnation created but never fenced — a crash (or
        // fence failure) between `create` and `sync_dir` in a prior open or
        // rotation leaves exactly that debris, and this boot-time fence
        // vouches for it before any new durability claim is made.
        disk.sync_dir()?;
        let end = LogOffset {
            segment_index: current_segment_index,
            byte_offset: current.len(),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                disk,
                config,
                current_segment_index,
                current,
                staged_end: end,
                durable_end: end,
                waiters: Vec::new(),
                poisoned: false,
                metrics: None,
            })),
        })
    }

    /// Install the session's shared metric registry ([STL-253]): subsequent
    /// appends count into `stele_wal_appends_total` and fsyncs observe
    /// `stele_wal_fsync_seconds`. Without one (the default) instrumentation is
    /// skipped entirely.
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    pub fn set_metrics(&self, metrics: SharedMetrics) {
        self.inner.lock().expect("wal mutex poisoned").metrics = Some(metrics);
    }

    /// Stage `payload` as a single record. Returns the [`LogOffset`] *after* the
    /// record — pass it to [`commit`](Self::commit) to await durability.
    ///
    /// The record is visible to subsequent reads on the same `Disk`
    /// immediately, but is not durable until an fsync covers it (via either
    /// [`tick`](Self::tick) or the segment-boundary sync inside rotation).
    ///
    /// # Errors
    ///
    /// [`WalError::PayloadTooLarge`] if `payload` exceeds the record limit;
    /// [`WalError::Poisoned`] if a prior fsync failed or an append tore (the WAL
    /// refuses further writes until recovery, [STL-217] / [STL-299]);
    /// [`WalError::Io`] on a backing write failure. A backing-write failure that
    /// *tore* — bytes physically landed past the staged end — additionally poisons
    /// the WAL before surfacing the [`WalError::Io`] ([STL-299]); a clean failure
    /// (nothing landed) does not.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    /// [STL-299]: https://allegromusic.atlassian.net/browse/STL-299
    // The guard is held across rotate + append + the `staged_end` bump and dropped
    // at the block's end *before* the wakers fire (waking can re-enter the mutex) —
    // the tightening clippy suggests would break that ordering. Same shape, and
    // same allow, as `drain_tick`.
    #[allow(clippy::significant_drop_tightening)]
    pub fn append(&self, payload: &[u8]) -> Result<LogOffset, WalError> {
        if payload.len() > MAX_PAYLOAD_LEN as usize {
            return Err(WalError::PayloadTooLarge(payload.len()));
        }
        let record_len = HEADER_LEN as u64 + payload.len() as u64;

        // Wakers to fire after the lock drops: a rotation's group-commit fsync may
        // wake covered waiters, and a torn append (or poisoning rotation) hands its
        // parked waiters back here so each re-polls and observes the poison.
        let mut wakers = Vec::new();
        let result = {
            let mut g = self.inner.lock().expect("wal mutex poisoned");

            // A poisoned WAL (a prior fsync or torn append) refuses every write, so a
            // later `tick` can never flush a staged record as a clean op ([STL-217]).
            if g.poisoned {
                Err(WalError::Poisoned)
            } else {
                // Rotate if appending this record would overflow the current
                // segment. A record is never split across segments. A rotation
                // fsync failure poisons and drains its parked waiters into
                // `wakers`, which the post-lock loop still fires.
                let projected = g.staged_end.byte_offset + record_len;
                let rotated =
                    if projected > g.config.segment_size_bytes && g.staged_end.byte_offset > 0 {
                        rotate(&mut g, &mut wakers)
                    } else {
                        Ok(())
                    };
                // Append the frame only if the rotation (if any) succeeded.
                rotated.and_then(|()| {
                    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
                    encode(payload, &mut frame);
                    if let Err(e) = g.current.append(&frame) {
                        // A *torn* append physically landed some bytes past
                        // `staged_end` — the segment is now longer than the WAL's
                        // bookkeeping records. Those stray bytes can never be safely
                        // built on: a later append would land at the physical EOF
                        // (past them) while `staged_end` advances from its stale
                        // value, desyncing the two, and recovery would shear at the
                        // torn frame and drop every record written after it. So
                        // poison the WAL — exactly like a failed fsync ([STL-299],
                        // [STL-217]) — and hand back every parked waiter so each
                        // re-polls to `Err(Poisoned)` rather than hanging. A *clean*
                        // failure (nothing landed; `len()` still equals `staged_end`)
                        // leaves the WAL consistent and is *not* poisoned — the caller
                        // rolls its resident writes back and keeps running ([STL-295]).
                        //
                        // This keys off the backend reporting its *actual* post-error
                        // length, and both shipped backends do: `MemDisk`'s torn fault
                        // advances `len()` by what landed, and `LocalFile::append`
                        // accumulates the bytes its `write` loop physically wrote even
                        // when the append then errors ([STL-305]). So a torn append now
                        // poisons on a real filesystem too, not only under the sim — a
                        // clean failure (`len()` still equals `staged_end`) does not.
                        if g.current.len() > g.staged_end.byte_offset {
                            g.poisoned = true;
                            wakers.extend(drain_all_waiters(&mut g));
                        }
                        return Err(WalError::Io(e));
                    }
                    g.staged_end.byte_offset += record_len;
                    if let Some(m) = &g.metrics {
                        m.wal_appends.inc();
                    }
                    Ok(g.staged_end)
                })
            }
        };
        // Wake any commit futures the lock scope touched — outside the lock, since
        // `wake` can re-enter the same mutex. A successful rotation made them
        // durable; a poisoning rotation or torn append hands them here so they
        // re-poll and observe the poison instead of hanging.
        for w in wakers {
            w.wake();
        }
        result
    }

    /// Return a future that resolves once every record appended **before** or
    /// **at** `target` is durable on disk.
    ///
    /// The future does not drive I/O on its own — durability is produced by
    /// [`tick`](Self::tick) (the group-commit fsync) or by the segment-boundary
    /// fsync inside rotation. One `tick()` may resolve many pending `commit()`
    /// futures: that's group commit.
    pub fn commit(&self, target: LogOffset) -> Commit<D> {
        Commit {
            inner: Arc::clone(&self.inner),
            target,
        }
    }

    /// Drain: write any buffered bytes (in this implementation they are already
    /// in the file handle, since `append` writes through), then `fsync`, then
    /// advance `durable_end` and wake every commit future whose target is now
    /// covered.
    ///
    /// Returns the number of commit waiters woken.
    ///
    /// # Errors
    ///
    /// [`WalError::Poisoned`] if a prior fsync already failed. If *this* fsync
    /// fails, the WAL is poisoned (so the staged record can never be flushed by a
    /// later `tick`) and the underlying [`WalError::Io`] is returned — the caller
    /// must treat it as a crash and recover, not as a clean abort ([STL-217]).
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    pub fn tick(&self) -> Result<usize, WalError> {
        // Collect wakers under the lock, then drop the guard before waking them —
        // waking a waker may re-acquire the same mutex (the woken task can race to
        // re-poll its Commit future). A failed fsync wakes the parked waiters too
        // (it hands them back here), so each re-polls and observes the poison
        // instead of hanging — no later `tick` would ever wake them ([STL-217]).
        let (wakers, result) = drain_tick(&self.inner);
        let woken = wakers.len();
        for w in wakers {
            w.wake();
        }
        result.map(|()| woken)
    }

    /// Replay records from `checkpoint` forward. Yields each record's payload
    /// as an owned `Vec<u8>` and stops on the first detected corruption
    /// (`Err(WalError::Io)` with kind `InvalidData`) — replay never proceeds
    /// past corruption.
    ///
    /// Idempotent: calling this multiple times produces the same sequence.
    pub fn replay_from(&self, checkpoint: Checkpoint) -> super::replay::Replay<D> {
        super::replay::Replay::new(Arc::clone(&self.inner), checkpoint)
    }

    /// Snapshot the current durable end of the log — usable as the next
    /// [`Checkpoint`].
    pub fn durable_end(&self) -> LogOffset {
        self.inner.lock().expect("wal mutex poisoned").durable_end
    }

    /// Whether the WAL is **poisoned** — a prior fsync ([`tick`](Self::tick) or
    /// the segment-boundary sync in rotation) failed ([STL-217]), or an
    /// [`append`](Self::append) tore (bytes landed past the staged end yet the
    /// call failed, [STL-299]) — so every further [`append`](Self::append) /
    /// [`tick`](Self::tick) is refused with [`WalError::Poisoned`] until the engine
    /// restarts into recovery.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    /// [STL-299]: https://allegromusic.atlassian.net/browse/STL-299
    pub fn is_poisoned(&self) -> bool {
        self.inner.lock().expect("wal mutex poisoned").poisoned
    }
}

// The whole drain needs to happen under the lock — every loop iteration both
// reads `g.waiters.len()` and may swap-remove. The clippy hint to drop the
// guard mid-loop is a false positive on this shape.
//
// Returns the wakers to fire **alongside** the result, rather than `?`-ing the
// error out, so the caller still wakes every parked waiter on a poison failure —
// otherwise a durability future parked before the failed fsync would hang
// forever (no later `tick` runs to wake it, [STL-217]).
#[allow(clippy::significant_drop_tightening)]
fn drain_tick<D: Disk>(inner: &Mutex<Inner<D>>) -> (Vec<Waker>, Result<(), WalError>) {
    let mut g = inner.lock().expect("wal mutex poisoned");
    if g.poisoned {
        return (Vec::new(), Err(WalError::Poisoned));
    }
    // A failed fsync leaves the staged tail of indeterminate durability — poison
    // the WAL *before* surfacing the error so no later `tick` advances
    // `durable_end` past it (which would flush the staged record under the guise
    // of an aborted op, [STL-217]). Hand back **every** parked waiter so the
    // caller wakes them and each re-polls to observe the poison (resolving
    // `Err(Poisoned)`) instead of hanging. The first failure still returns the
    // concrete I/O error; subsequent calls get `WalError::Poisoned`.
    let fsync_started = g.metrics.as_ref().map(|m| m.now_micros());
    if let Err(e) = g.current.sync() {
        g.poisoned = true;
        return (drain_all_waiters(&mut g), Err(WalError::Io(e)));
    }
    observe_fsync(g.metrics.as_ref(), fsync_started);
    g.durable_end = g.staged_end;
    (drain_waiters(&mut g), Ok(()))
}

/// Remove and return wakers for every waiter whose target is now ≤
/// `durable_end`. Multi-waker waiters (multiple futures awaiting the same
/// offset) contribute every one of their wakers.
fn drain_waiters<D: Disk>(g: &mut Inner<D>) -> Vec<Waker> {
    let mut wakers = Vec::new();
    let mut i = 0;
    while i < g.waiters.len() {
        if g.waiters[i].target <= g.durable_end {
            let w = g.waiters.swap_remove(i);
            wakers.extend(w.wakers);
        } else {
            i += 1;
        }
    }
    wakers
}

/// Remove and return **every** waiter's wakers, regardless of target — used when
/// the WAL poisons. Each parked durability future then re-polls and resolves
/// `Err(Poisoned)` rather than hanging, since no later `tick` or rotation will
/// ever wake it ([STL-217]).
fn drain_all_waiters<D: Disk>(g: &mut Inner<D>) -> Vec<Waker> {
    g.waiters.drain(..).flat_map(|w| w.wakers).collect()
}

/// Rotate to a fresh segment. The `wakers` out-parameter accumulates any
/// commit waiters the closing segment's fsync just made durable; the caller is
/// responsible for waking them *after* releasing the mutex.
fn rotate<D: Disk>(g: &mut Inner<D>, wakers: &mut Vec<Waker>) -> Result<(), WalError> {
    // Sync the closing segment FIRST. If `create` ran first and `sync` then
    // failed, we'd leave an orphan empty segment with a higher index — a later
    // `Wal::open` would pick that index as the head and silently skip past the
    // closing segment's unsynced tail, breaking recovery.
    //
    // A failed boundary fsync is the same crash the group-commit `tick` faces:
    // poison the WAL so no further write proceeds ([STL-217]). Poison before
    // returning, so the staged-but-unsynced closing segment is never advanced
    // past by a later `tick`. Drain every parked waiter into the out-param so the
    // caller wakes them (this runs inside `append`'s lock scope, whose post-lock
    // wake loop fires `wakers` on the error path too) — otherwise a durability
    // future parked before the rotation would hang.
    let fsync_started = g.metrics.as_ref().map(|m| m.now_micros());
    if let Err(e) = g.current.sync() {
        g.poisoned = true;
        wakers.extend(drain_all_waiters(g));
        return Err(WalError::Io(e));
    }
    observe_fsync(g.metrics.as_ref(), fsync_started);
    let new_idx = g.current_segment_index + 1;
    let new_file = g.disk.create(&segment::name_for(new_idx))?;
    // Directory fence ([STL-232]): the new segment's *entry* must be durable
    // before any record fsync'd into it can count as durable — recovery finds
    // segments by listing the disk, so a synced record in an unlinked file
    // would be silently lost. A failed fence is a failed fsync: poison, same
    // as the closing-segment sync above ([STL-217]). The unfenced file this
    // leaves behind is healed by [`Wal::open`]'s unconditional fence on the
    // post-poison restart, before the reopened WAL claims any durability.
    if let Err(e) = g.disk.sync_dir() {
        g.poisoned = true;
        wakers.extend(drain_all_waiters(g));
        return Err(WalError::Io(e));
    }

    // From here on the rotation is committed. The closing segment is durable,
    // so every record in it is durable — advance `durable_end` past the
    // boundary and drain waiters covered by that fsync. (Without this, a
    // commit() future awaiting the closing segment would sit Pending until the
    // next tick(), even though its data is already on disk — which violates
    // the contract that "any fsync that covers a target resolves it".)
    g.current_segment_index = new_idx;
    g.current = new_file;
    g.staged_end = LogOffset {
        segment_index: new_idx,
        byte_offset: 0,
    };
    g.durable_end = g.staged_end;
    wakers.extend(drain_waiters(g));
    Ok(())
}

/// Observe one successful fsync into the registry's WAL-fsync histogram
/// ([STL-253]). `started` is the pre-sync reading paired with the registry it
/// came from; both `None` (no registry installed) skip the observation. With a
/// registry but no installed time source both readings are `0`, so the
/// duration observes as zero — the count still ticks, deterministically.
///
/// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
fn observe_fsync(metrics: Option<&SharedMetrics>, started: Option<u64>) {
    if let (Some(m), Some(started)) = (metrics, started) {
        m.wal_fsync_seconds
            .observe_micros(m.now_micros().saturating_sub(started));
    }
}

fn list_segments<D: Disk>(disk: &D) -> io::Result<Vec<u64>> {
    let mut indices: Vec<u64> = disk
        .list()?
        .iter()
        .filter_map(|name| segment::index_of(name))
        .collect();
    indices.sort_unstable();
    Ok(indices)
}

/// The future returned by [`Wal::commit`].
pub struct Commit<D: Disk> {
    inner: Arc<Mutex<Inner<D>>>,
    target: LogOffset,
}

impl<D: Disk> Future for Commit<D> {
    type Output = Result<(), WalError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let mut g = me.inner.lock().expect("wal mutex poisoned");
        if me.target <= g.durable_end {
            return Poll::Ready(Ok(()));
        }
        // A poisoned WAL will never advance `durable_end` again, so a record not
        // already durable can never become durable on this instance — resolve the
        // wait with an error rather than parking forever ([STL-217]). A record
        // *already* past the fence resolved `Ok` above, even after poison.
        if g.poisoned {
            return Poll::Ready(Err(WalError::Poisoned));
        }
        // Park ourselves. Multiple `Commit` futures may share the same
        // `target` (e.g. two consumers awaiting the same boundary); we store
        // every distinct waker so each gets notified, deduped by
        // `Waker::will_wake` so repeated polls from the same task don't bloat
        // the vector.
        let new_waker = cx.waker().clone();
        for w in &mut g.waiters {
            if w.target == me.target {
                if !w
                    .wakers
                    .iter()
                    .any(|existing| existing.will_wake(&new_waker))
                {
                    w.wakers.push(new_waker);
                }
                return Poll::Pending;
            }
        }
        g.waiters.push(Waiter {
            target: me.target,
            wakers: vec![new_waker],
        });
        Poll::Pending
    }
}

pub(crate) fn read_segment<D: Disk>(
    inner: &Mutex<Inner<D>>,
    segment_index: u64,
    offset: u64,
    buf: &mut [u8],
) -> io::Result<usize> {
    let g = inner.lock().expect("wal mutex poisoned");
    if segment_index == g.current_segment_index {
        return g.current.read_at(offset, buf);
    }
    // For older segments we have to open a fresh handle. Replay is a cold path;
    // re-opening per read is acceptable for v0.1.
    let file = g.disk.open(&segment::name_for(segment_index))?;
    drop(g);
    file.read_at(offset, buf)
}

pub(crate) fn segment_len<D: Disk>(inner: &Mutex<Inner<D>>, segment_index: u64) -> io::Result<u64> {
    let g = inner.lock().expect("wal mutex poisoned");
    if segment_index == g.current_segment_index {
        return Ok(g.current.len());
    }
    let file = g.disk.open(&segment::name_for(segment_index))?;
    drop(g);
    Ok(file.len())
}

pub(crate) fn known_segments<D: Disk>(inner: &Mutex<Inner<D>>) -> io::Result<Vec<u64>> {
    let g = inner.lock().expect("wal mutex poisoned");
    list_segments(&g.disk)
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Waker};

    use super::*;
    use crate::backend::{FaultOp, Faults, MemDisk};

    /// A failed group-commit fsync poisons the WAL: the staged record is **not**
    /// flushed by a later `tick` (the durability hazard [STL-217] closes), and
    /// every further write is refused. The first failure surfaces the concrete
    /// I/O error; subsequent calls report [`WalError::Poisoned`].
    #[test]
    fn a_failed_fsync_poisons_and_refuses_further_writes() {
        let faults = Faults::new();
        // Exactly the *next* sync fails — the group-commit `tick` below.
        faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
        let wal = Wal::open(MemDisk::with_faults(faults), WalConfig::default()).expect("open");

        let staged = wal
            .append(b"committed-but-unsynced")
            .expect("append stages");
        assert!(
            staged > LogOffset::ZERO,
            "the record is staged past the origin"
        );

        // The fsync fails: the first failure is the concrete I/O error, and the WAL
        // is now poisoned with nothing made durable.
        let err = wal
            .tick()
            .expect_err("the injected fsync fault fails the tick");
        assert!(
            matches!(err, WalError::Io(_)),
            "first failure is the io error"
        );
        assert!(wal.is_poisoned(), "a failed fsync poisons the WAL");
        assert_eq!(wal.durable_end(), LogOffset::ZERO, "nothing became durable",);

        // The crux: a *later* tick must not flush the staged record. The scheduled
        // fault was consumed by the first sync, so without poison this second tick
        // would succeed and advance `durable_end` past the staged record — exactly
        // the "aborted op silently becomes durable" hazard. Poison refuses it.
        let err = wal.tick().expect_err("a poisoned tick is refused");
        assert!(matches!(err, WalError::Poisoned));
        assert_eq!(
            wal.durable_end(),
            LogOffset::ZERO,
            "the poisoned tick did not flush the staged record",
        );

        // Appends are refused too, so no new record stacks on the staged one.
        let err = wal
            .append(b"after poison")
            .expect_err("a poisoned append is refused");
        assert!(matches!(err, WalError::Poisoned));
    }

    /// A failed *segment-boundary* fsync (inside rotation) poisons just like the
    /// group-commit `tick` — the same crash, a different sync site.
    #[test]
    fn a_failed_rotation_fsync_poisons() {
        let faults = Faults::new();
        faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
        // A 1-byte segment bound forces the second append to rotate first.
        let config = WalConfig {
            segment_size_bytes: 1,
        };
        let wal = Wal::open(MemDisk::with_faults(faults), config).expect("open");

        // First append never rotates (the segment is empty); it just stages.
        wal.append(b"first").expect("first append stages");
        // Second append would overflow the 1-byte segment, so it rotates — and the
        // closing segment's fsync hits the scheduled fault.
        let err = wal
            .append(b"second")
            .expect_err("rotation fsync fails the append");
        assert!(matches!(err, WalError::Io(_)));
        assert!(wal.is_poisoned(), "a failed rotation fsync poisons the WAL");

        let err = wal
            .append(b"third")
            .expect_err("a poisoned append is refused");
        assert!(matches!(err, WalError::Poisoned));
    }

    /// A durability future for a record that never became durable resolves with an
    /// error once the WAL is poisoned, rather than parking forever; a record already
    /// past the durable fence still resolves `Ok` even after poison.
    #[test]
    fn commit_future_after_poison_errors_for_unsynced_and_ok_for_durable() {
        let faults = Faults::new();
        let wal =
            Wal::open(MemDisk::with_faults(faults.clone()), WalConfig::default()).expect("open");

        // One durable record establishes a fence to test the "already durable" arm.
        let durable = wal.append(b"durable").expect("append");
        wal.tick().expect("clean fsync");
        assert_eq!(wal.durable_end(), durable);

        // A second record is staged, then its fsync fails and poisons the WAL.
        let staged = wal.append(b"staged").expect("append");
        faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
        wal.tick().expect_err("the fsync fault poisons");
        assert!(wal.is_poisoned());

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        // The already-durable target resolves Ok even after poison.
        let fut = wal.commit(durable);
        let mut fut = pin!(fut);
        assert!(
            matches!(fut.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
            "a record past the fence is durable regardless of poison",
        );

        // The unsynced target can never become durable on this instance — error, not hang.
        let fut = wal.commit(staged);
        let mut fut = pin!(fut);
        assert!(
            matches!(
                fut.as_mut().poll(&mut cx),
                Poll::Ready(Err(WalError::Poisoned))
            ),
            "an unsynced record's durability wait resolves Poisoned",
        );
    }

    /// A durability future **parked before** a failed fsync must be *woken* by the
    /// poison and then resolve `Err(Poisoned)` — otherwise it would sit Pending
    /// forever, since no later `tick` runs to wake it. Covers the `tick` and the
    /// rotation poison sites.
    #[test]
    fn poison_wakes_a_waiter_parked_before_the_failed_fsync() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::Wake;

        /// A waker that records whether it was woken.
        struct FlagWaker(AtomicBool);
        impl Wake for FlagWaker {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        // Park a waiter, fail `target`'s fsync via `tick`, and assert it is woken
        // and resolves Poisoned. `rotate` shares the same poison-and-drain path.
        let faults = Faults::new();
        let wal =
            Wal::open(MemDisk::with_faults(faults.clone()), WalConfig::default()).expect("open");

        let staged = wal.append(b"staged").expect("append");
        let flag = Arc::new(FlagWaker(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);
        let fut = wal.commit(staged);
        let mut fut = pin!(fut);
        assert!(
            fut.as_mut().poll(&mut cx).is_pending(),
            "the unsynced target parks (registering its waker)",
        );

        // The fsync fails: poisoning must wake the parked waiter.
        faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
        wal.tick().expect_err("the fsync fault poisons");
        assert!(
            flag.0.load(Ordering::SeqCst),
            "poisoning wakes the parked durability waiter",
        );

        // The woken waiter, re-polled, resolves Poisoned rather than hanging.
        assert!(
            matches!(
                fut.as_mut().poll(&mut cx),
                Poll::Ready(Err(WalError::Poisoned))
            ),
            "the woken waiter resolves Poisoned",
        );
    }

    /// A **torn** append — bytes physically land past the staged end, yet the
    /// call fails — poisons the WAL ([STL-299]): the stray bytes can never be
    /// built on, so every further append/tick is refused. This is the append-side
    /// analogue of the fsync poison ([STL-217]). The first failure surfaces the
    /// concrete I/O error; subsequent calls report [`WalError::Poisoned`].
    #[test]
    fn a_torn_append_poisons_and_refuses_further_writes() {
        let faults = Faults::new();
        let wal =
            Wal::open(MemDisk::with_faults(faults.clone()), WalConfig::default()).expect("open");

        // One clean, durable record establishes a committed prefix the poison must
        // leave untouched (it is the consistent state recovery converges to).
        let durable = wal.append(b"committed").expect("append");
        wal.tick().expect("fsync");
        assert_eq!(wal.durable_end(), durable);

        // The next append is torn: it lands a few stray bytes, then fails. The
        // first failure is the concrete I/O error, and the WAL is now poisoned with
        // nothing past the committed prefix made durable.
        faults.schedule_torn_append(io::ErrorKind::Other, 4);
        let err = wal
            .append(b"torn-record")
            .expect_err("the torn append fails");
        assert!(
            matches!(err, WalError::Io(_)),
            "first failure is the io error",
        );
        assert!(wal.is_poisoned(), "a torn append poisons the WAL");
        assert_eq!(
            wal.durable_end(),
            durable,
            "nothing past the committed prefix became durable",
        );

        // The crux: no later write may build on the staged garbage. A subsequent
        // append lands at the physical EOF (past the stray bytes) while `staged_end`
        // still points before them — exactly the desync the poison forecloses.
        let err = wal
            .append(b"after")
            .expect_err("a poisoned append is refused");
        assert!(matches!(err, WalError::Poisoned));
        let err = wal.tick().expect_err("a poisoned tick is refused");
        assert!(matches!(err, WalError::Poisoned));
    }

    /// A **clean** append failure — the fault fires before any byte lands — does
    /// **not** poison: the WAL's bookkeeping still matches the file, so the caller
    /// rolls its resident writes back and keeps running ([STL-295]). This pins the
    /// boundary the torn-append poison must not cross — only a *detectably-torn*
    /// append (bytes landed) is a crash.
    #[test]
    fn a_clean_append_failure_does_not_poison() {
        let faults = Faults::new();
        let wal =
            Wal::open(MemDisk::with_faults(faults.clone()), WalConfig::default()).expect("open");

        let durable = wal.append(b"committed").expect("append");
        wal.tick().expect("fsync");

        // A clean append failure: nothing lands (the default schedule's prefix is 0).
        faults.schedule(FaultOp::Append, io::ErrorKind::Other);
        let err = wal.append(b"refused").expect_err("the clean append fails");
        assert!(matches!(err, WalError::Io(_)));
        assert!(
            !wal.is_poisoned(),
            "a clean append failure leaves the WAL healthy",
        );

        // The WAL keeps working: a subsequent append lands at the right offset and
        // a fsync makes it durable — proving its bookkeeping never desynced.
        let next = wal.append(b"next").expect("a healthy WAL still appends");
        assert!(next > durable);
        wal.tick().expect("fsync");
        assert_eq!(
            wal.durable_end(),
            next,
            "the post-failure write is durable — no stray bytes were skipped",
        );
    }
}
