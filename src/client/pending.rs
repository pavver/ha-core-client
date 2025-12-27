use super::events::HistoricalState;
use super::state::HaState;
use anyhow::Result;
use tokio::sync::oneshot;

// --- Structs and enums for managing pending API calls ---

pub(crate) struct PendingGetStateOnceCall {
    pub(super) responder: oneshot::Sender<Result<HaState>>,
}

pub(crate) struct PendingGetHistoryCall {
    pub(super) entity_id: String,
    pub(super) responder: oneshot::Sender<Result<Vec<HistoricalState>>>,
}

pub(crate) struct PendingServiceCall {
    pub(super) responder: oneshot::Sender<Result<()>>,
}

pub(crate) struct PendingUnsubscribeCall {
    pub(super) responder: oneshot::Sender<Result<()>>,
}

pub(crate) enum PendingCall {
    GetStateOnce(PendingGetStateOnceCall),
    ServiceCall(PendingServiceCall),
    GetHistory(PendingGetHistoryCall),
    Unsubscribe(PendingUnsubscribeCall),
}
