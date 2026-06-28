//! Link state and link flow state

use std::{cell::Cell, marker::PhantomData, sync::Arc};

use fe2o3_amqp_types::definitions::{Fields, SequenceNo};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::{
    endpoint::{LinkFlow, OutputHandle},
    util::{Consume, ProducerState},
};

use super::{role, ReceiverTransferError, SenderFlowState};

thread_local! {
    static CUSTOM_DELIVERY_TAG: Cell<Option<[u8; 16]>> = const { Cell::new(None) };
}

/// Sets a custom 16-byte delivery tag to be used for the next message transfer.
///
/// When set, the next call to `generate_delivery_tag` will return this custom tag
/// instead of generating a new UUID. This is used by the emulator to ensure the
/// delivery tag matches a lock token stored in the queue, so the Azure SDK's
/// renew-lock requests find the right message.
///
/// The custom tag is consumed (cleared) after being used once.
#[allow(dead_code)]
pub fn set_custom_delivery_tag(tag: [u8; 16]) {
    CUSTOM_DELIVERY_TAG.with(|t| t.set(Some(tag)));
}

/// Clears any custom delivery tag that was set via `set_custom_delivery_tag`.
///
/// After clearing, the next call to `generate_delivery_tag` will generate a new
/// random UUID instead of using the custom tag.
pub fn clear_custom_delivery_tag() {
    CUSTOM_DELIVERY_TAG.with(|t| t.set(None));
}

/// Link state.
///
/// There is no official definition of the link state in the specification
#[derive(Debug)]
pub enum LinkState {
    /// The initial state after initialization
    Unattached,

    /// An attach frame has been sent
    AttachSent,

    /// An attach has been sent but with incomplete unsettled
    IncompleteAttachSent,

    /// An attach frame has been received
    AttachReceived,

    /// An attach frame has been received with incomplete unsettled
    IncompleteAttachReceived,

    /// The link is attached
    Attached,

    /// The two endpoints has exchanged Attach frames but at least one of them is incomplete
    IncompleteAttachExchanged,

    /// A non-closing detach frame has been sent
    DetachSent,

    /// A non-closing detach frame has been received
    DetachReceived,

    /// The link is detached
    Detached,

    /// A closing detach frame has been sent
    CloseSent,

    /// A closing detach has arrived
    CloseReceived,

    /// The link is closed
    Closed,
}

#[derive(Debug)]
pub(crate) struct LinkFlowStateInner {
    pub initial_delivery_count: SequenceNo,
    pub delivery_count: SequenceNo, // SequenceNo = u32
    pub link_credit: u32,
    pub available: u32,
    pub drain: bool,
    pub properties: Option<Fields>,
}

impl LinkFlowStateInner {
    pub fn as_link_flow(
        &self,
        output_handle: OutputHandle,
        echo: bool,
        include_properties: bool,
    ) -> LinkFlow {
        let properties = if include_properties {
            self.properties.clone()
        } else {
            None
        };

        LinkFlow {
            handle: output_handle.into(),
            delivery_count: Some(self.delivery_count),
            link_credit: Some(self.link_credit),
            available: Some(self.available),
            drain: self.drain,
            echo,
            properties,
        }
    }
}

/// The Sender and Receiver handle link flow control differently
#[derive(Debug)]
pub(crate) struct LinkFlowState<R> {
    pub(crate) lock: RwLock<LinkFlowStateInner>,
    role: PhantomData<R>,
}

impl<R> LinkFlowState<R> {
    pub(crate) fn new(inner: LinkFlowStateInner) -> Self {
        Self {
            lock: RwLock::new(inner),
            role: PhantomData,
        }
    }
}

impl LinkFlowState<role::SenderMarker> {
    pub(crate) fn sender(inner: LinkFlowStateInner) -> Self {
        Self::new(inner)
    }
}

impl LinkFlowState<role::ReceiverMarker> {
    pub(crate) fn receiver(inner: LinkFlowStateInner) -> Self {
        Self::new(inner)
    }
}

impl LinkFlowState<role::SenderMarker> {
    /// Handles incoming Flow frame
    ///
    /// If an echo (reply with the local flow state) is requested, return an `Ok(Some(Flow))`,
    /// otherwise, return a `Ok(None)`
    #[inline]
    pub(crate) fn on_incoming_flow(
        &self,
        flow: LinkFlow,
        output_handle: OutputHandle,
    ) -> Option<LinkFlow> {
        let mut state = self.lock.write();

        // delivery count
        //
        // ...
        // Only the sender MAY independently modify this field.

        // link credit
        //
        // ...
        // This means that the sender’s link-credit variable
        // MUST be set according to this formula when flow information is given by the
        // receiver:
        // link-credit_snd := delivery-count_rcv + link-credit_rcv - delivery-count_snd.
        let delivery_count_rcv = flow.delivery_count.unwrap_or(
            // In the event that the receiver does not yet know the delivery-count,
            // i.e., delivery-count_rcv is unspecified, the sender MUST assume that
            // the delivery-count_rcv is the first delivery-count_snd sent from sender
            // to receiver, i.e., the delivery-count_snd specified in the flow state
            // carried by the initial attach frame from the sender to the receiver.
            state.initial_delivery_count,
        );

        if let Some(link_credit_rcv) = flow.link_credit {
            let link_credit = delivery_count_rcv
                .saturating_add(link_credit_rcv)
                .saturating_sub(state.delivery_count);
            state.link_credit = link_credit;
        }

        // available
        //
        // The available variable is controlled by the sender, and indicates to the receiver,
        // that the sender could make use of the indicated amount of link-credit. Only the
        // sender can indepen- dently modify this field.

        // The drain flag indicates how the sender SHOULD behave when insufficient messages
        // are available to consume the current link-credit. If set, the sender will (after
        // sending all available messages) advance the delivery-count as much as possible,
        // consuming all link-credit, and send the flow state to the receiver. Only the
        // receiver can independently modify this field. The sender’s value is always the
        // last known value indicated by the receiver.
        state.drain = flow.drain;
        if flow.drain {
            state.delivery_count = state.delivery_count.wrapping_add(state.link_credit);
            state.link_credit = 0;

            return Some(state.as_link_flow(output_handle, false, false));
        }

        match flow.echo {
            // Should avoid constant ping-pong
            true => Some(state.as_link_flow(output_handle, false, false)),
            false => None,
        }
    }
}

impl LinkFlowState<role::ReceiverMarker> {
    #[inline]
    pub(crate) fn on_incoming_flow(
        &self,
        flow: LinkFlow,
        output_handle: OutputHandle,
    ) -> Option<LinkFlow> {
        let mut state = self.lock.write();

        // delivery count
        //
        // The receiver’s value is calculated based on the last known
        // value from the sender and any subsequent messages received on the link. Note that,
        // despite its name, the delivery-count is not a count but a sequence number
        // initialized at an arbitrary point by the sender.
        if let Some(delivery_count) = flow.delivery_count {
            state.delivery_count = delivery_count;
        }

        // link credit
        //
        // Only the receiver can independently choose a value for this field. The sender’s
        // value MUST always be maintained in such a way as to match the delivery-limit
        // identified by the receiver.

        // available
        //
        // The receiver’s value is calculated
        // based on the last known value from the sender and any subsequent incoming
        // messages received. The sender MAY transfer messages even if the available variable
        // is zero. If this happens, the receiver MUST maintain a floor of zero in its
        // calculation of the value of available.
        if let Some(available) = flow.available {
            state.available = available;
        }

        // drain
        //
        // The drain flag indicates how the sender SHOULD behave when insufficient messages
        // are available to consume the current link-credit. If set, the sender will (after
        // sending all available messages) advance the delivery-count as much as possible,
        // consuming all link-credit, and send the flow state to the receiver. Only the
        // receiver can independently modify this field. The sender’s value is always the
        // last known value indicated by the receiver.

        match flow.echo {
            true => Some(state.as_link_flow(output_handle, false, false)),
            false => None,
        }
    }
}

impl<R> LinkFlowState<R> {
    pub fn link_credit(&self) -> u32 {
        self.lock.read().link_credit
    }

    pub fn drain(&self) -> bool {
        self.lock.read().drain
    }

    pub fn initial_delivery_count(&self) -> SequenceNo {
        self.lock.read().initial_delivery_count
    }

    pub fn initial_delivery_count_mut(&self, f: impl Fn(u32) -> u32) {
        let mut guard = self.lock.write();
        let new = f(guard.initial_delivery_count);
        guard.initial_delivery_count = new;
    }

    pub fn delivery_count_mut(&self, f: impl Fn(u32) -> u32) {
        let mut guard = self.lock.write();
        let new = f(guard.delivery_count);
        guard.delivery_count = new;
    }

    /// This is async because it is protected behind an async RwLock
    pub fn properties(&self) -> Option<Fields> {
        self.lock.read().properties.clone()
    }
}

impl LinkFlowState<role::ReceiverMarker> {
    /// Consume one link credit if available. Returns an error if there is
    /// not enough link credit
    pub fn consume(&self, count: u32) -> Result<(), ReceiverTransferError> {
        let mut state = self.lock.write();
        if state.link_credit < count {
            Err(ReceiverTransferError::TransferLimitExceeded)
        } else {
            state.delivery_count = state.delivery_count.wrapping_add(count);
            state.link_credit = state.link_credit.saturating_sub(count);
            Ok(())
        }
    }
}

impl ProducerState for Arc<LinkFlowState<role::SenderMarker>> {
    type Item = (LinkFlow, OutputHandle);
    // If echo is requested, a Some(LinkFlow) will be returned
    type Outcome = Option<LinkFlow>;

    #[inline]
    fn update_state(&mut self, (flow, output_handle): Self::Item) -> Self::Outcome {
        self.on_incoming_flow(flow, output_handle)
    }
}

#[derive(Debug)]
struct InsufficientCredit {}

impl Consume for SenderFlowState {
    type Item = u32;
    type Outcome = [u8; 16];

    /// Increment delivery count and decrement link_credit. Wait asynchronously
    /// if there is not enough credit
    ///
    /// # Cancel safety
    ///
    /// `Notify` itself is not cancel safe in the way that it would lose its place in the queue.
    /// However, since there can be only one consumer for a producer, losing the place in the queue
    /// does not have any effect. Thus, this IS cancel safe.
    async fn consume(&mut self, item: Self::Item) -> Self::Outcome {
        loop {
            match consume_link_credit(&self.state().lock, item) {
                Ok(outcome) => return outcome,
                Err(_) => self.notifier.notified().await, // **NOT** cancel safe
            }
        }
    }
}

cfg_transaction! {
    impl crate::util::TryConsume for SenderFlowState {
        type Error = super::error::SenderTryConsumeError;

        fn try_consume(&mut self, item: Self::Item) -> Result<Self::Outcome, Self::Error> {
            let mut state = self
                .state()
                .lock
                .try_write()
                .ok_or(super::error::SenderTryConsumeError::TryLockError)?;
            if state.link_credit < item {
                Err(Self::Error::InsufficientCredit)
            } else {
                let tag = generate_delivery_tag(&mut state);
                Ok(tag)
            }
        }
    }
}

/// Generate a 16-byte UUID delivery tag for Azure SDK compatibility.
/// The Azure Python SDK expects delivery tags to be 16-byte UUIDs, not
/// 4-byte counters. This ensures the lock_token property works correctly.
///
/// The SDK interprets delivery tag bytes as little-endian UUID
/// (`uuid.UUID(bytes_le=self._delivery_tag)`), so we store the UUID bytes
/// in little-endian order so the SDK sees the same UUID we store as the
/// lock token.
///
/// If a custom delivery tag has been set via `set_custom_delivery_tag`,
/// that tag is used instead of generating a new UUID. This allows the
/// caller to ensure the delivery tag matches a lock token stored in the
/// queue, so the Azure SDK's renew-lock requests find the right message.
fn generate_delivery_tag(state: &mut LinkFlowStateInner) -> [u8; 16] {
    state.delivery_count = state.delivery_count.wrapping_add(1);
    state.link_credit = state.link_credit.saturating_sub(1);

    if let Some(tag) = CUSTOM_DELIVERY_TAG.with(|t| t.get()) {
        clear_custom_delivery_tag();
        return tag;
    }

    let uuid = Uuid::new_v4();
    // Convert big-endian UUID bytes to little-endian format so the Azure SDK
    // (which uses uuid.UUID(bytes_le=delivery_tag)) sees the same UUID.
    let bytes = uuid.as_bytes();
    [
        bytes[3], bytes[2], bytes[1], bytes[0], bytes[5], bytes[4], bytes[7], bytes[6], bytes[8],
        bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]
}

fn consume_link_credit(
    lock: &RwLock<LinkFlowStateInner>,
    count: u32,
) -> Result<[u8; 16], InsufficientCredit> {
    let mut state = lock.write();

    if state.link_credit < count {
        Err(InsufficientCredit {})
    } else {
        let tag = generate_delivery_tag(&mut state);
        Ok(tag)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures_util::poll;
    use tokio::{sync::Notify, time::timeout};

    use crate::{
        endpoint::{LinkFlow, OutputHandle},
        link::{
            role,
            state::{LinkFlowState, LinkFlowStateInner},
            SenderFlowState,
        },
        util::{Consume, Consumer, Produce, Producer},
    };

    macro_rules! assert_pending {
        ($fut:expr) => {
            let fut = Box::pin($fut);
            let poll = poll!(fut);
            assert!(poll.is_pending());
        };
    }

    macro_rules! assert_ready {
        ($fut:expr) => {
            let fut = Box::pin($fut);
            let poll = poll!(fut);
            assert!(poll.is_ready());
        };
    }

    fn create_sender_flow_state_producer_and_consumer() -> (
        Producer<Arc<LinkFlowState<role::SenderMarker>>>,
        SenderFlowState,
    ) {
        let flow_state_inner = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 0,
            available: 0,
            drain: false,
            properties: None,
        };
        let flow_state = Arc::new(LinkFlowState::sender(flow_state_inner));
        let notifier = Arc::new(Notify::new());
        let producer = Producer::new(notifier.clone(), flow_state.clone());
        let consumer = Consumer::new(notifier, flow_state);
        (producer, consumer)
    }

    #[tokio::test]
    async fn test_sender_flow_state_producer_and_consumer() {
        let (mut producer, mut consumer) = create_sender_flow_state_producer_and_consumer();

        // .await on notify
        assert_pending!(consumer.consume(1));

        // .await after notify with zero credit
        let item = (LinkFlow::default(), OutputHandle(0));
        producer.produce(item).await;
        assert_pending!(consumer.consume(1));

        // .await after notify with non-zero credit
        let link_flow = LinkFlow {
            link_credit: Some(2),
            ..Default::default()
        };
        let item = (link_flow, OutputHandle(0));
        producer.produce(item).await;
        assert_ready!(consumer.consume(1));
        assert_ready!(consumer.consume(1));

        // All credits have been consumed already
        assert_pending!(consumer.consume(1));
    }

    #[tokio::test]
    async fn test_spawned_flow_state_producer_and_consumer() {
        let (mut producer, mut consumer) = create_sender_flow_state_producer_and_consumer();

        let handle = tokio::spawn(async move { consumer.consume(1).await });
        let mut fut = Box::pin(handle);
        assert_pending!(&mut fut);

        // .await after notify with non-zero credit
        let link_flow = LinkFlow {
            link_credit: Some(1),
            ..Default::default()
        };
        let item = (link_flow, OutputHandle(0));
        producer.produce(item).await;

        let wait = timeout(Duration::from_millis(500), fut).await;
        assert!(wait.is_ok());
    }

    #[tokio::test]
    async fn test_drop_consume_fut_before_produce() {
        let (mut producer, mut consumer) = create_sender_flow_state_producer_and_consumer();

        // Drop before notify
        let fut = consumer.consume(1);
        let mut pinned = Box::pin(fut);
        assert!(poll!(&mut pinned).is_pending());
        drop(pinned);

        // .await after notify with non-zero credit
        let link_flow = LinkFlow {
            link_credit: Some(2),
            ..Default::default()
        };
        let item = (link_flow, OutputHandle(0));
        producer.produce(item).await;

        // If it is not cancel safe, we cannot consume all 2 credits
        assert_ready!(consumer.consume(1));
        assert_ready!(consumer.consume(1));

        // All credits have been consumed already
        assert_pending!(consumer.consume(1));
    }

    #[tokio::test]
    async fn test_drop_consume_fut_after_produce() {
        let (mut producer, mut consumer) = create_sender_flow_state_producer_and_consumer();

        // Drop before notify
        let fut = consumer.consume(1);
        let mut pinned = Box::pin(fut);
        assert!(poll!(&mut pinned).is_pending());

        // .await after notify with non-zero credit
        let link_flow = LinkFlow {
            link_credit: Some(2),
            ..Default::default()
        };
        let item = (link_flow, OutputHandle(0));
        producer.produce(item).await;

        drop(pinned);

        // If it is not cancel safe, we cannot consume all 2 credits
        assert_ready!(consumer.consume(1));
        assert_ready!(consumer.consume(1));

        // All credits have been consumed already
        assert_pending!(consumer.consume(1));
    }

    #[test]
    fn test_generate_delivery_tag_returns_16_bytes() {
        let mut state = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 5,
            link_credit: 10,
            available: 0,
            drain: false,
            properties: None,
        };

        let tag = super::generate_delivery_tag(&mut state);
        assert_eq!(tag.len(), 16);
        // Should not be all zeros
        assert_ne!(tag, [0u8; 16]);
    }

    #[test]
    fn test_generate_delivery_tag_is_unique() {
        let mut state1 = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 100,
            available: 0,
            drain: false,
            properties: None,
        };
        let mut state2 = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 50,
            link_credit: 100,
            available: 0,
            drain: false,
            properties: None,
        };

        let tag1 = super::generate_delivery_tag(&mut state1);
        let tag2 = super::generate_delivery_tag(&mut state2);
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn test_generate_delivery_tag_increments_delivery_count() {
        let mut state = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 10,
            link_credit: 5,
            available: 0,
            drain: false,
            properties: None,
        };

        let _ = super::generate_delivery_tag(&mut state);
        assert_eq!(state.delivery_count, 11);
        assert_eq!(state.link_credit, 4);
    }

    #[test]
    fn test_custom_delivery_tag_is_used() {
        let mut state = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 10,
            available: 0,
            drain: false,
            properties: None,
        };

        let custom = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        super::set_custom_delivery_tag(custom);

        let tag = super::generate_delivery_tag(&mut state);
        assert_eq!(tag, custom);
    }

    #[test]
    fn test_custom_delivery_tag_consumed_once() {
        let mut state = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 10,
            available: 0,
            drain: false,
            properties: None,
        };

        let custom = [0xFF; 16];
        super::set_custom_delivery_tag(custom);

        // First call uses the custom tag
        let tag1 = super::generate_delivery_tag(&mut state);
        assert_eq!(tag1, custom);

        // Second call generates a new random tag
        let tag2 = super::generate_delivery_tag(&mut state);
        assert_ne!(tag2, custom);
    }

    #[test]
    fn test_clear_custom_delivery_tag() {
        let mut state = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 10,
            available: 0,
            drain: false,
            properties: None,
        };

        let custom = [0xAB; 16];
        super::set_custom_delivery_tag(custom);
        super::clear_custom_delivery_tag();

        // Should get a random tag, not the cleared custom one
        let tag = super::generate_delivery_tag(&mut state);
        assert_ne!(tag, custom);
    }

    #[test]
    fn test_generate_delivery_tag_uuid_v4_format() {
        let mut state = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 10,
            available: 0,
            drain: false,
            properties: None,
        };

        let tag = super::generate_delivery_tag(&mut state);

        // UUID version 4 has the version nibble set to 4 in byte[7] (after byte swap)
        // In the little-endian output, the version nibble (bits 12-15 of the
        // original UUID clock_seq_hi_and_reserved byte) ends up at tag[7] >> 4.
        // Actually the original UUID is at bytes index 6 (clock_seq_hi_and_reserved).
        // In the byte-swapped output (little-endian), tag[7] = bytes[7] which is
        // clock_seq_hi_and_reserved unchanged, so tag[7] >> 4 should be 4.
        assert_eq!(tag[7] >> 4, 0x4, "UUID version should be 4");

        // The variant bits (top 2 bits of clock_seq_hi_and_reserved at tag[8]) should be 10xxxxxx
        let variant = tag[8] >> 6;
        assert!(
            variant == 0x2,
            "UUID variant should be RFC 4122 (0x2), got {variant}"
        );
    }

    #[tokio::test]
    async fn test_consume_async_returns_16_bytes() {
        let (mut producer, mut consumer) = create_sender_flow_state_producer_and_consumer();

        let link_flow = LinkFlow {
            link_credit: Some(1),
            ..Default::default()
        };
        producer.produce((link_flow, OutputHandle(0))).await;

        let tag = consumer.consume(1).await;
        assert_eq!(tag.len(), 16);
        assert_ne!(tag, [0u8; 16]);

        // Should be a valid UUID v4
        assert_eq!(tag[7] >> 4, 0x4);
    }

    #[tokio::test]
    async fn test_consume_async_uses_custom_delivery_tag() {
        let (mut producer, mut consumer) = create_sender_flow_state_producer_and_consumer();

        let custom = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
            0xff, 0x00,
        ];
        super::set_custom_delivery_tag(custom);

        let link_flow = LinkFlow {
            link_credit: Some(2),
            ..Default::default()
        };
        producer.produce((link_flow, OutputHandle(0))).await;

        let tag1 = consumer.consume(1).await;
        assert_eq!(tag1, custom, "first consume should use custom tag");

        let tag2 = consumer.consume(1).await;
        assert_ne!(tag2, custom, "second consume should use random tag");
        assert_eq!(tag2.len(), 16);
    }

    #[test]
    fn test_consume_link_credit_returns_16_bytes() {
        let inner = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 5,
            available: 0,
            drain: false,
            properties: None,
        };
        let lock = Arc::new(parking_lot::RwLock::new(inner));

        let result = super::consume_link_credit(&lock, 1);
        assert!(result.is_ok());
        let tag = result.unwrap();
        assert_eq!(tag.len(), 16);
        assert_ne!(tag, [0u8; 16]);
        assert_eq!(tag[7] >> 4, 0x4);
    }

    #[test]
    fn test_consume_link_credit_insufficient_credit() {
        let inner = LinkFlowStateInner {
            initial_delivery_count: 0,
            delivery_count: 0,
            link_credit: 0,
            available: 0,
            drain: false,
            properties: None,
        };
        let lock = parking_lot::RwLock::new(inner);

        let result = super::consume_link_credit(&lock, 1);
        assert!(result.is_err());
    }
}
