use std::io::{self, Read, Write};

use actix_web::web::Bytes;
use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use tokio::task::spawn_blocking;

use crate::error::ProcessError;

// Profiles are kept gzipped, which is how hypixel sends them, so they can be passed on to clients that accept gzip without any work.

/// Gzips `data`, for when hypixel didn't. Profiles can be megabytes, so callers run this off the thread that is answering requests.
pub fn gzip_blocking(data: &[u8]) -> Result<Bytes, ProcessError> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(data.len() / 2), Compression::fast());
    encoder.write_all(data).and_then(|()| encoder.finish())
        .map(Bytes::from)
        .map_err(|e| ProcessError::Serialization(e.to_string()))
}

/// Unzips `data` on this thread, for callers that are already off the thread answering requests.
pub fn gunzip_blocking(data: &[u8]) -> io::Result<Vec<u8>> {
    // gzip ends with the unzipped size, so the output is allocated once. Capped in case the data is bogus.
    let size = data.last_chunk::<4>().map_or(0, |size| u32::from_le_bytes(*size) as usize).min(data.len() * 32);
    let mut out = Vec::with_capacity(size);
    MultiGzDecoder::new(data).read_to_end(&mut out)?;
    Ok(out)
}

/// Unzips `data`, for clients that don't accept gzip. Off the thread that is answering requests, as profiles can be megabytes.
pub async fn gunzip(data: Bytes) -> Result<Bytes, ProcessError> {
    spawn_blocking(move || gunzip_blocking(&data)).await
        .map_err(|_| ProcessError::InternalServer("Decompress task failed!"))?
        .map(Bytes::from)
        .map_err(|e| ProcessError::Serialization(e.to_string()))
}

/// A cheap check that `data` is gzip that hasn't been zeroed or overwritten, for entries read back from disk. It looks at the gzip header
/// and the unzipped size at the end, as unzipping all of it on every read would cost as much as the read itself.
pub fn looks_like_gzip(data: &[u8]) -> bool {
    const HEADER: [u8; 3] = [0x1f, 0x8b, 0x08]; // magic bytes and deflate.
    const MIN_LEN: usize = 18; // a 10 byte header and an 8 byte trailer.
    // profiles are never empty, so an unzipped size of zero means the end of the entry was zeroed.
    data.len() > MIN_LEN && data.starts_with(&HEADER) && data.last_chunk::<4>().is_some_and(|size| *size != [0; 4])
}
