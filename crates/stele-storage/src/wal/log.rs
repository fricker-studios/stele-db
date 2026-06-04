//! The WAL: append, group-commit fsync, segment rotation.
//!
//! See [`crate::wal`] for the durability contract.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::disk::{Disk, DiskFile};
use super::record::{HEADER_LEN, MAX_PAYLOAD_LEN, encode};
use super::segment;

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
        let (current_segment_index, current) = match segments.last().copied() {
            Some(idx) => (idx, disk.open(&segment::name_for(idx))?),
            None => (0, disk.create(&segment::name_for(0))?),
        };
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
            })),
        })
    }

    /// Stage `payload` as a single record. Returns the [`LogOffset`] *after* the
    /// record — pass it to [`commit`](Self::commit) to await durability.
    ///
    /// The record is visible to subsequent reads on the same `Disk`
    /// immediately, but is not durable until an fsync covers it (via either
    /// [`tick`](Self::tick) or the segment-boundary sync inside rotation).
    pub fn append(&self, payload: &[u8]) -> Result<LogOffset, WalError> {
        if payload.len() > MAX_PAYLOAD_LEN as usize {
            return Err(WalError::PayloadTooLarge(payload.len()));
        }
        let record_len = HEADER_LEN as u64 + payload.len() as u64;

        let mut rotation_wakers = Vec::new();
        let result = {
            let mut g = self.inner.lock().expect("wal mutex poisoned");

            // Rotate if appending this record would overflow the current
            // segment. A record is never split across segments.
            let projected = g.staged_end.byte_offset + record_len;
            if projected > g.config.segment_size_bytes && g.staged_end.byte_offset > 0 {
                rotate(&mut g, &mut rotation_wakers)?;
            }

            let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
            encode(payload, &mut frame);
            g.current.append(&frame)?;
            g.staged_end.byte_offset += record_len;
            g.staged_end
        };
        // Wake any commit futures the rotation's fsync just made durable —
        // outside the lock, since `wake` can re-enter the same mutex.
        for w in rotation_wakers {
            w.wake();
        }
        Ok(result)
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
    pub fn tick(&self) -> Result<usize, WalError> {
        // Collect ready wakers under the lock, then drop the guard before waking
        // them — waking a waker may re-acquire the same mutex (the woken task
        // can race to re-poll its Commit future).
        let wakers = drain_tick(&self.inner)?;
        let woken = wakers.len();
        for w in wakers {
            w.wake();
        }
        Ok(woken)
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
}

// The whole drain needs to happen under the lock — every loop iteration both
// reads `g.waiters.len()` and may swap-remove. The clippy hint to drop the
// guard mid-loop is a false positive on this shape.
#[allow(clippy::significant_drop_tightening)]
fn drain_tick<D: Disk>(inner: &Mutex<Inner<D>>) -> Result<Vec<Waker>, WalError> {
    let mut g = inner.lock().expect("wal mutex poisoned");
    g.current.sync()?;
    g.durable_end = g.staged_end;
    Ok(drain_waiters(&mut g))
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

/// Rotate to a fresh segment. The `wakers` out-parameter accumulates any
/// commit waiters the closing segment's fsync just made durable; the caller is
/// responsible for waking them *after* releasing the mutex.
fn rotate<D: Disk>(g: &mut Inner<D>, wakers: &mut Vec<Waker>) -> Result<(), WalError> {
    // Sync the closing segment FIRST. If `create` ran first and `sync` then
    // failed, we'd leave an orphan empty segment with a higher index — a later
    // `Wal::open` would pick that index as the head and silently skip past the
    // closing segment's unsynced tail, breaking recovery.
    g.current.sync()?;
    let new_idx = g.current_segment_index + 1;
    let new_file = g.disk.create(&segment::name_for(new_idx))?;

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
