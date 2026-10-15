//! Turns 10 ms audio frames into fixed-size PCM 16-bit little-endian chunks.

/// Sample rate sent to the speech-to-text service.
pub const SAMPLE_RATE: u32 = 16_000;
/// 40 ms at 16 kHz: small enough for low latency, large enough to keep WebSocket overhead low.
pub const CHUNK_SAMPLES: usize = 640;

/// Accumulates mono i16 samples and yields `CHUNK_SAMPLES`-sized little-endian byte chunks.
#[derive(Default)]
pub struct Chunker {
    pending: Vec<i16>,
}

impl Chunker {
    pub fn push(&mut self, samples: &[i16]) -> Vec<Vec<u8>> {
        self.pending.extend_from_slice(samples);
        let mut out = Vec::new();
        while self.pending.len() >= CHUNK_SAMPLES {
            let chunk: Vec<i16> = self.pending.drain(..CHUNK_SAMPLES).collect();
            out.push(to_le_bytes(&chunk));
        }
        out
    }
}

pub fn to_le_bytes(samples: &[i16]) -> Vec<u8> {
    samples.iter().flat_map(|s| s.to_le_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::{CHUNK_SAMPLES, Chunker, to_le_bytes};

    #[test]
    fn encodes_little_endian() {
        assert_eq!(to_le_bytes(&[1, -2]), vec![0x01, 0x00, 0xfe, 0xff]);
    }

    #[test]
    fn emits_full_chunks_only() {
        let mut c = Chunker::default();
        let frame = vec![7i16; 160];
        assert!(c.push(&frame).is_empty());
        assert!(c.push(&frame).is_empty());
        assert!(c.push(&frame).is_empty());
        let out = c.push(&frame);
        assert_eq!(out.len(), 1);
        assert_eq!(out.first().map(Vec::len), Some(CHUNK_SAMPLES * 2));
    }
}
