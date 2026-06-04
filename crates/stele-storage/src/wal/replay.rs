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
}

impl<D: Disk> Replay<D> {
    pub(super) fn new(inner: Arc<Mutex<Inner<D>>>, checkpoint: Checkpoint) -> Self {
        let segments = known_segments(&inner).unwrap_or_default();
        Self {
            inner,
            position: checkpoint.0,
            segments,
            stopped: false,
        }
    }

    fn read_next(&mut self) -> Result<Option<Vec<u8>>, WalError> {
        loop {
            // Advance over segments whose tail we've passed.
            let seg = self.position.segment_index;
            if !self.segments.contains(&seg) {
                return Ok(None);
            }
            let len = segment_len(&self.inner, seg)?;
            if self.position.byte_offset >= len {
                // Move to the next segment if one exists; otherwise we're done.
                let Some(&next) = self.segments.iter().find(|&&candidate| candidate > seg) else {
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
