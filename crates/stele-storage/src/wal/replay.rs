//! Replay iterator — walks segments forward from a checkpoint.
//!
//! On the **first** CRC-mismatched (or short-read) record, replay yields
//! `Err(WalError::Io { kind: InvalidData })` and then `None` forever. This is
//! the torn-write contract: corruption is detected, and replay refuses to
//! continue past it.

use std::io;
use std::sync::{Arc, Mutex};

use super::disk::Disk;
use super::log::{
    Checkpoint, Inner, LogOffset, WalError, known_segments, read_segment, segment_len,
};
use super::record::{HEADER_LEN, MAX_PAYLOAD_LEN, parse_header, verify};

/// Iterator over recovered records. See [`crate::wal::Wal::replay_from`].
pub struct Replay<D: Disk> {
    inner: Arc<Mutex<Inner<D>>>,
    position: LogOffset,
    segments: Vec<u64>,
    stopped: bool,
    /// Error captured at construction time (e.g. `Disk::list` failed). Yielded
    /// on the first `next()` call and then cleared; the iterator stops after.
    init_error: Option<WalError>,
}

impl<D: Disk> Replay<D> {
    pub(super) fn new(inner: Arc<Mutex<Inner<D>>>, checkpoint: Checkpoint) -> Self {
        let (segments, init_error) = match known_segments(&inner) {
            Ok(s) => (s, None),
            Err(e) => (Vec::new(), Some(WalError::Io(e))),
        };
        Self {
            inner,
            position: checkpoint.0,
            segments,
            stopped: false,
            init_error,
        }
    }

    fn read_next(&mut self) -> Result<Option<Vec<u8>>, WalError> {
        loop {
            // `self.segments` is sorted (produced by `known_segments`), so we
            // can binary-search the current position and step forward in O(1)
            // — keeping replay O(records · log segments) rather than O(records
            // · segments).
            let seg = self.position.segment_index;
            let Ok(seg_idx) = self.segments.binary_search(&seg) else {
                return Ok(None);
            };
            let len = segment_len(&self.inner, seg)?;
            if self.position.byte_offset >= len {
                let Some(&next) = self.segments.get(seg_idx + 1) else {
                    return Ok(None);
                };
                self.position = LogOffset {
                    segment_index: next,
                    byte_offset: 0,
                };
                continue;
            }

            return self.read_record(seg, len);
        }
    }

    fn read_record(&mut self, seg: u64, segment_len: u64) -> Result<Option<Vec<u8>>, WalError> {
        let remaining = segment_len - self.position.byte_offset;
        if remaining < HEADER_LEN as u64 {
            // Tail short of a complete header — treat as torn-write corruption.
            return Err(corrupt("incomplete record header at log tail"));
        }
        let mut header_buf = [0u8; HEADER_LEN];
        let read = read_segment(&self.inner, seg, self.position.byte_offset, &mut header_buf)?;
        if read != HEADER_LEN {
            return Err(corrupt("short read on record header"));
        }
        let header = parse_header(&header_buf).expect("8 bytes parses");
        if header.len > MAX_PAYLOAD_LEN {
            return Err(corrupt("declared record length exceeds MAX_PAYLOAD_LEN"));
        }
        let record_total = HEADER_LEN as u64 + u64::from(header.len);
        if record_total > remaining {
            return Err(corrupt("record payload exceeds segment tail"));
        }

        let mut payload = vec![0u8; header.len as usize];
        let payload_offset = self.position.byte_offset + HEADER_LEN as u64;
        let read = read_segment(&self.inner, seg, payload_offset, &mut payload)?;
        if read != payload.len() {
            return Err(corrupt("short read on record payload"));
        }
        if !verify(header, &payload) {
            return Err(corrupt("crc32c mismatch"));
        }

        self.position = LogOffset {
            segment_index: seg,
            byte_offset: self.position.byte_offset + record_total,
        };
        Ok(Some(payload))
    }
}

impl<D: Disk> Iterator for Replay<D> {
    type Item = Result<Vec<u8>, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }
        // Surface any construction-time error first, exactly once, then stop —
        // same "yield Err, then None forever" contract as corruption detection.
        if let Some(e) = self.init_error.take() {
            self.stopped = true;
            return Some(Err(e));
        }
        match self.read_next() {
            Ok(Some(payload)) => Some(Ok(payload)),
            Ok(None) => None,
            Err(e) => {
                self.stopped = true;
                Some(Err(e))
            }
        }
    }
}

fn corrupt(msg: &'static str) -> WalError {
    WalError::Io(io::Error::new(io::ErrorKind::InvalidData, msg))
}
