use anyhow::Result;
use serde_json::Value;
use tokio::sync::oneshot;

// --- Structs and enums for managing pending API calls ---

pub(super) struct PendingGetStateCall {
    pub(super) entity_id: String,
    pub(super) responder: oneshot::Sender<Result<Option<Value>>>,
}

pub(super) struct PendingServiceCall {
    pub(super) responder: oneshot::Sender<Result<()>>,
}

pub(super) enum PendingCall {
    GetState(PendingGetStateCall),
    ServiceCall(PendingServiceCall),
}
