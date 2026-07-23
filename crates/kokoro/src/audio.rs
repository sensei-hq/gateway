//! Encode Kokoro's `f32` PCM output as a WAV file. Shared across all languages.

/// Kokoro's output audio sample rate (Hz).
pub const SAMPLE_RATE: u32 = 24_000;

/// Encode mono `f32` PCM samples in `[-1.0, 1.0]` (at [`SAMPLE_RATE`]) as a
/// 16-bit PCM WAV byte buffer. Samples are clamped to `[-1, 1]` then rounded to
/// `i16`.
pub fn pcm_f32_to_wav(samples: &[f32]) -> Vec<u8> {
    const BITS_PER_SAMPLE: u16 = 16;
    const CHANNELS: u16 = 1;
    const HEADER_LEN: usize = 44;

    let block_align = CHANNELS * (BITS_PER_SAMPLE / 8);
    let byte_rate = SAMPLE_RATE * u32::from(block_align);
    let data_len = (samples.len() * 2) as u32;

    let mut out = Vec::with_capacity(HEADER_LEN + samples.len() * 2);
    // RIFF header.
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_len).to_le_bytes());
    out.extend_from_slice(b"WAVE");
    // fmt chunk (PCM).
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk body size
    out.extend_from_slice(&1u16.to_le_bytes()); // audio format 1 = PCM
    out.extend_from_slice(&CHANNELS.to_le_bytes());
    out.extend_from_slice(&SAMPLE_RATE.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&BITS_PER_SAMPLE.to_le_bytes());
    // data chunk.
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    for &s in samples {
        let v = (s.clamp(-1.0, 1.0) * f32::from(i16::MAX)).round() as i16;
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_a_well_formed_pcm_header() {
        let wav = pcm_f32_to_wav(&[0.0, 0.5, -0.5]);
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(&wav[12..16], b"fmt ");
        assert_eq!(&wav[36..40], b"data");
        // audio format PCM (1), mono (1), 24 kHz.
        assert_eq!(u16::from_le_bytes([wav[20], wav[21]]), 1);
        assert_eq!(u16::from_le_bytes([wav[22], wav[23]]), 1);
        assert_eq!(
            u32::from_le_bytes([wav[24], wav[25], wav[26], wav[27]]),
            SAMPLE_RATE
        );
        // 44-byte header + 2 bytes per sample.
        assert_eq!(wav.len(), 44 + 3 * 2);
    }

    #[test]
    fn clamps_and_scales_samples() {
        let wav = pcm_f32_to_wav(&[2.0, -2.0]); // out of range → clamped
        let s0 = i16::from_le_bytes([wav[44], wav[45]]);
        let s1 = i16::from_le_bytes([wav[46], wav[47]]);
        assert_eq!(s0, i16::MAX);
        assert_eq!(s1, -i16::MAX); // -1.0 * 32767
    }
}
