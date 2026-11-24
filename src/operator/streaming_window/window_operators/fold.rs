use std::collections::HashMap;
use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::{ChainTag, WindowItem, WindowTag};
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

/// Folds the items of each window into one accumulator, kept in a map keyed by `(key, window tag)`.
///
/// A `Close` emits the accumulator of that window, tagged with the parent tag. The result is
/// timestamped with the timestamp of the `Close` (the end of the window), if it has one, otherwise
/// with the largest timestamp of the folded items.
/// Watermarks are forwarded immediately: results of closed windows always precede them.
/// At the end of the stream the accumulators of windows that are still open are emitted too.
/// The `Close` of a window is consumed, the one of the enclosing window is forwarded.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<K, T, I, O, F, Op>
where
    O: Send + Clone,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = WindowItem<K, T, I>>,
    T: WindowTag,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: O,
    accumulators: HashMap<(K, T), (O, Option<Timestamp>)>,
    #[derivative(Debug = "ignore")]
    ready: Vec<StreamElement<WindowItem<K, T::Parent, O>>>,
    received_end: bool,
    received_end_iter: bool,
}

impl<K, T, I, O, F, Op> Display for Fold<K, T, I, O, F, Op>
where
    O: Data,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = WindowItem<K, T, I>>,
    I: Data,
    K: DataKey,
    T: WindowTag,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Fold<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<K, T, I, O, F, Op> Fold<K, T, I, O, F, Op>
where
    O: Data,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = WindowItem<K, T, I>>,
    K: DataKey,
    T: WindowTag,
{
    pub(crate) fn new(prev: Op, init: O, fold: F) -> Self {
        Fold {
            prev,
            fold,
            init,
            accumulators: HashMap::new(),
            ready: Vec::new(),
            received_end: false,
            received_end_iter: false,
        }
    }

    fn accumulate(&mut self, key: K, tag: T, item: I, ts: Option<Timestamp>) {
        let (acc, acc_ts) = self
            .accumulators
            .entry((key, tag))
            .or_insert_with(|| (self.init.clone(), None));
        if let Some(ts) = ts {
            *acc_ts = Some(acc_ts.unwrap_or(ts).max(ts));
        }
        (self.fold)(acc, item);
    }
}

/// Output element for a finished window: the item moves up to the parent window.
fn emit<K, T: WindowTag, O>(
    key: K,
    tag: T,
    acc: O,
    ts: Option<Timestamp>,
) -> StreamElement<WindowItem<K, T::Parent, O>> {
    let el = WindowItem::new_item(key, tag.into_parent(), acc);
    StreamElement::new_item(el, ts)
}

impl<K, T, I, O, F, Op> Fold<K, ChainTag<T>, I, O, F, Op>
where
    O: Data,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = WindowItem<K, ChainTag<T>, I>>,
    K: DataKey,
    T: WindowTag,
{
    /// Handle the `Close` of the window `tag`, or of its enclosing window (see `ChainTag`).
    fn close(
        &mut self,
        key: K,
        tag: ChainTag<T>,
        ts: Option<Timestamp>,
    ) -> Option<StreamElement<WindowItem<K, T, O>>> {
        if tag.is_parent_tag() {
            // all the sub-windows of the enclosing window were already emitted
            let close = WindowItem::new_close(key, tag.into_parent());
            return Some(StreamElement::new_item(close, ts));
        }
        let (acc, acc_ts) = self.accumulators.remove(&(key.clone(), tag.clone()))?;
        Some(emit(key, tag, acc, ts.or(acc_ts)))
    }
}

impl<K, T, I, O, F, Op> Operator for Fold<K, ChainTag<T>, I, O, F, Op>
where
    O: Data,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = WindowItem<K, ChainTag<T>, I>>,
    I: Data,
    T: WindowTag,
    K: DataKey,
{
    type Out = WindowItem<K, T, O>;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Item(WindowItem::Item { key, tag, item }) => {
                    self.accumulate(key, tag, item, None)
                }
                StreamElement::Timestamped(WindowItem::Item { key, tag, item }, ts) => {
                    self.accumulate(key, tag, item, Some(ts))
                }
                StreamElement::Item(WindowItem::Close { key, tag }) => {
                    if let Some(el) = self.close(key, tag, None) {
                        return el;
                    }
                }
                StreamElement::Timestamped(WindowItem::Close { key, tag }, ts) => {
                    if let Some(el) = self.close(key, tag, Some(ts)) {
                        return el;
                    }
                }
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                // this block won't send anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        // end of stream: emit the windows that were never closed
        self.ready.extend(
            self.accumulators
                .drain()
                .map(|((key, tag), (acc, ts))| emit(key, tag, acc, ts)),
        );

        if let Some(elem) = self.ready.pop() {
            return elem;
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("StreamingWindowFold"))
    }
}
