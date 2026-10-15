pub(crate) fn object(kind: &str, id: &str) -> workflow_foundation::ObjectKey {
    workflow_foundation::ObjectKey::new(kind, id)
}
pub(crate) fn reject(message: &str) -> workflow_foundation::Failure {
    workflow_foundation::Failure::Rejected(message.into())
}
impl From<crate::domains::DomainError> for workflow_foundation::Failure {
    fn from(error: crate::domains::DomainError) -> Self {
        Self::Rejected(error.0)
    }
}
