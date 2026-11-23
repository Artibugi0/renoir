use std::collections::VecDeque;
use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::{
    ChainTag, InnerWindowItem, NoTag, WindowItem, WindowTag,
};
use crate::operator::window::{KeyedWindowTagger, WindowTagger};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::ExecutionMetadata;

/// Assigns each incoming item to the windows produced by a `WindowTagger`.
///
/// One tagger is kept per `(key, parent tag)`. For the first window level the parent tag is `()`,
/// for nested levels it is the tag of the enclosing window.
/// Output items are tagged with `ChainTag<parent>`.
#[derive(Clone)]
pub(crate) struct WindowRouter<K, T1, T2, I, Op, W>
where
    W: WindowTagger,
{
    prev: Op,
    /// Used only for tracing.
    name: String,
    tagger: KeyedWindowTagger<K, T1, I, W>,
    output_buffer: VecDeque<StreamElement<WindowItem<K, T2, I>>>,
}

impl<K, T1, T2, I, Prev, W> WindowRouter<K, T1, T2, I, Prev, W>
where
    W: WindowTagger,
{
    pub(crate) fn new(prev: Prev, name: &str, tagger: KeyedWindowTagger<K, T1, I, W>) -> Self {
        Self {
            prev,
            name: name.into(),
            tagger,
            output_buffer: Default::default(),
        }
    }
}

impl<K, T1, T2, I, Prev, W> Display for WindowRouter<K, T1, T2, I, Prev, W>
where
    W: WindowTagger,
    Prev: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {} -> WindowRouter[{}]<{}>",
            self.prev,
            std::any::type_name::<W>(),
            self.name,
            std::any::type_name::<WindowItem<K, T2, I>>(),
        )
    }
}

/// Logic shared by the first-level and the nested router.
impl<K, T, P, I, Prev, W> WindowRouter<K, T, ChainTag<P>, I, Prev, W>
where
    K: DataKey,
    T: Eq + std::hash::Hash + Clone,
    P: Clone,
    I: Data,
    W: WindowTagger,
{
    /// Feed `el` to the tagger of `(key, tag)` and buffer the tagged outputs.
    fn route(&mut self, key: K, tag: T, parent: P, el: StreamElement<InnerWindowItem<(), I>>) {
        // a close of the enclosing window resets the tagger: drop it instead of keeping it forever
        let closes = matches!(el.item(), Some(InnerWindowItem::Close { .. }));
        let tagger = self
            .tagger
            .windows
            .entry((key.clone(), tag.clone()))
            .or_insert_with(|| self.tagger.init.clone());
        let ret = tagger.tag(el);
        self.output_buffer.extend(ret.into_iter().map(|el| {
            StreamElement::Item(WindowItem::from_parts(key.clone(), parent.clone(), el))
        }));
        if closes {
            self.tagger.windows.remove(&(key.clone(), tag));
            // pass the close on, so the next level can close its own window
            self.output_buffer.push_back(StreamElement::Item(WindowItem::new_close(
                key,
                ChainTag::parent_tag(parent),
            )));
        }
    }

    /// Feed a system message (watermark, terminate, flush) to every tagger, drop the ones that
    /// are done, and forward the message after any items it released.
    fn broadcast<X>(&mut self, el: &StreamElement<X>, parent_of: impl Fn(&T) -> P) {
        self.tagger.windows.retain(|(key, tag), tagger| {
            let ret = tagger.tag(el.as_system().expect("not an item"));
            let parent = parent_of(tag);
            self.output_buffer.extend(ret.into_iter().map(|el| {
                StreamElement::Item(WindowItem::from_parts(key.clone(), parent.clone(), el))
            }));
            !tagger.recycle()
        });
        self.output_buffer
            .push_back(el.as_system().expect("not an item"));
    }
}

/// First window level: input is a plain keyed stream.
impl<K, I, Prev, W> Operator for WindowRouter<K, (), ChainTag<NoTag>, I, Prev, W>
where
    W: WindowTagger + Send,
    Prev: Operator<Out = (K, I)>,
    K: DataKey,
    I: Data,
{
    type Out = WindowItem<K, ChainTag<NoTag>, I>;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                return item;
            }
            match self.prev.next() {
                el @ (StreamElement::Item(_) | StreamElement::Timestamped(..)) => {
                    // TODO: Handle timestamps
                    let (key, el) = el.take_key();
                    let el = el.map(|el| InnerWindowItem::new_item((), el));
                    self.route(key.unwrap(), (), NoTag, el);
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                el => self.broadcast(&el, |_| NoTag),
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>(&self.name))
    }
}

/// Nested window level: input is already tagged by the enclosing window.
impl<K, I, T, Prev, W> Operator for WindowRouter<K, T, ChainTag<T>, I, Prev, W>
where
    W: WindowTagger,
    Prev: Operator<Out = WindowItem<K, T, I>>,
    K: DataKey,
    T: WindowTag,
    I: Data,
{
    type Out = WindowItem<K, ChainTag<T>, I>;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                return item;
            }
            match self.prev.next() {
                el @ (StreamElement::Item(_) | StreamElement::Timestamped(..)) => {
                    // TODO: Handle timestamps
                    let item = el.item().unwrap();
                    let (key, tag) = (item.key().clone(), item.tag().clone());
                    self.route(key, tag.clone(), tag, el.map(WindowItem::strip));
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                el => self.broadcast(&el, T::clone),
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>(&self.name))
    }
}
