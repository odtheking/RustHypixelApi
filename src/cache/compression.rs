use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use actix_web::web::Bytes;
use tokio::task::spawn_blocking;

use crate::error::ProcessError;

/// Profiles can be megabytes, so this is done off the thread that is answering requests.
pub async fn compress(data: Bytes) -> Result<Vec<u8>, ProcessError> {
    spawn_blocking(move || compress_prepend_size(&data)).await
        .map_err(|_| ProcessError::InternalServer("Compress task failed!"))
}

/// expected input should be gathered from `compress()`
pub async fn decompress(data: impl AsRef<[u8]> + Send + 'static) -> Result<Vec<u8>, ProcessError> {
    spawn_blocking(move || decompress_size_prepended(data.as_ref())).await
        .map_err(|_| ProcessError::InternalServer("Decompress task failed!"))?
        .map_err(|e| ProcessError::Database(e.to_string()))
}
