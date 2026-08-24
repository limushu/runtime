//! A small service runtime for control-plane workflows.
//!
//! Business developers use four concepts only: service, message, task and router.

mod message;
mod observation;
mod protocol;
mod router;
mod service;
mod task;

pub mod demo;

pub use message::{HandlerRegistry, Message, MessagePayload};
pub use observation::{
    ServiceActivity, ServiceLifecycle, ServiceObserver, ServiceSnapshot, TaskEvent, TaskState,
    TaskVisibility,
};
pub use protocol::{
    MessageContext, OperationId, Reply, RuntimeError, TaskId, TaskKey, Ticket, TraceContext,
    request_channel,
};
pub use router::Router;
pub use service::{
    CommandHandle, ControlHandle, PoolServices, ServiceContext, ServiceEntry, ServiceKey,
    ServiceRef, ServiceShutdown,
};
pub use task::{CancelMode, CancelReason, RunOutcome, TaskContext, TaskOutcome};

/// Defines the closed message protocol used by all services in a pool.
///
/// Every payload type maps to one enum variant and therefore one static message kind.
#[macro_export]
macro_rules! define_messages {
    (
        $vis:vis enum $message:ident => $kind:ident {
            $( $variant:ident($payload:ty) ),+ $(,)?
        }
    ) => {
        #[derive(Debug)]
        $vis enum $message {
            $( $variant($payload) ),+
        }

        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        $vis enum $kind {
            $( $variant ),+
        }

        impl $crate::Message for $message {
            type Kind = $kind;

            fn kind(&self) -> Self::Kind {
                match self {
                    $( Self::$variant(_) => $kind::$variant ),+
                }
            }
        }

        $(
            impl $crate::MessagePayload<$message> for $payload {
                const KIND: $kind = $kind::$variant;

                fn into_message(self) -> $message {
                    $message::$variant(self)
                }

                fn from_message(message: $message) -> Result<Self, $message> {
                    match message {
                        $message::$variant(payload) => Ok(payload),
                        other => Err(other),
                    }
                }
            }
        )+
    };
}

/// Installs a feature's complete payload-to-handler mapping in one place.
#[macro_export]
macro_rules! register_handlers {
    ($registry:expr, { $( $payload:ty => $handler:path ),+ $(,)? }) => {
        (|| -> Result<(), $crate::RuntimeError> {
            $( $registry.on::<$payload, _>($handler)?; )+
            Ok(())
        })()
    };
}
