use crate::RuntimeError;

use super::super::{BgBackend, BgId};

pub async fn rebuild(backend: BgBackend, bg: BgId) -> Result<(), RuntimeError> {
    backend.rebuild(bg).await;
    Ok(())
}
