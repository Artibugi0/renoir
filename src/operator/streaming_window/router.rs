use std::collections::VecDeque;
use std::fmt::Display;

use crate::block::OperatorStructure;
use crate::operator::streaming_window::streamingwindow::{
    ChainTag, InnerWindowItem, NoTag, WindowItem, WindowTag,
};
// use crate::operator::streaming_window::window_operators::fold::NestedFold;
use crate::operator::window::{KeyedWindowTagger, WindowTagger};
use crate::operator::{Data, DataKey, StreamElement};

use crate::operator::Operator;
/// This operator abstracts the window logic as an operator and delegates to the
/// `KeyedWindowManager` and a `ProcessFunc` the job of building and processing the windows,
/// respectively.
#[derive(Clone)]
pub(crate) struct WindowRouter<K, T1, T2, I, Op, W>
where
    W: WindowTagger,
{
    /// The previous operators in the chain.
    prev: Op,
    /// The name of the actual operator that this one abstracts.
    ///
    /// It is used only for tracing purposes.
    name: String,
    /// The manager that will build the windows.
    tagger: KeyedWindowTagger<K, T1, I, W>,
    /// A buffer for storing ready items.
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

impl<K, I, Prev, W> Operator for WindowRouter<K, (), ChainTag<NoTag>, I, Prev, W>
where
    W: WindowTagger + Send,
    Prev: Operator<Out = (K, I)>,
    K: DataKey,
    // T: WindowTag,
    I: Data,
{
    type Out = WindowItem<K, ChainTag<NoTag>, I>;

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> crate::operator::StreamElement<Self::Out> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                return item;
            }

            let el = self.prev.next();
            match el {
                el @ (StreamElement::Item(_) | StreamElement::Timestamped(_, _)) => {
                    let (key, el) = el.take_key(); // TODO: Handle timestamps
                    let key = key.unwrap();

                    let tgr = self
                        .tagger
                        .windows
                        .entry((key.clone(), ()))
                        .or_insert_with(|| self.tagger.init.clone());

                    let window_el = el.map(|el| InnerWindowItem::new_item((), el));
                    let ret = tgr.tag(window_el);
                    self.output_buffer.extend(
                        ret.into_iter()
                            .map(|el| WindowItem::from_parts(key.clone(), NoTag, el))
                            .map(StreamElement::Item),
                    );
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                el => {
                    let (_, el) = el.take_key();

                    self.tagger.windows.retain(|kt, mgr| {
                        let window_el = el.clone().map(|el| InnerWindowItem::new_item((), el));
                        let ret = mgr.tag(window_el);
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|el| WindowItem::from_parts(kt.0.clone(), NoTag, el))
                                .map(StreamElement::Item),
                        );
                        !mgr.recycle()
                    });

                    // Forward system messages and watermarks
                    let msg = match el {
                        StreamElement::Watermark(w) => StreamElement::Watermark(w),
                        StreamElement::Terminate => StreamElement::Terminate,
                        StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
                        _ => unreachable!(),
                    };
                    self.output_buffer.push_back(msg);
                }
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(
                OperatorStructure::new::<WindowItem<K, ChainTag<NoTag>, I>, _>(&self.name),
            )
    }
}

impl<K, I, T, Prev, W> Operator for WindowRouter<K, T, ChainTag<T>, I, Prev, W>
where
    W: WindowTagger,
    Prev: Operator<Out = WindowItem<K, T, I>>,
    K: DataKey,
    T: WindowTag,
    I: Data,
{
    type Out = WindowItem<K, ChainTag<T>, I>;

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> crate::operator::StreamElement<Self::Out> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                return item;
            }

            let el = self.prev.next();
            match el {
                el @ (StreamElement::Item(_) | StreamElement::Timestamped(_, _)) => {
                    //TODO: Handle timestamps
                    let (key, tag) = el
                        .item()
                        .map(|item| (item.key().clone(), item.tag().clone()))
                        .unwrap();

                    let tgr = self
                        .tagger
                        .windows
                        .entry((key.clone(), tag.clone()))
                        .or_insert_with(|| self.tagger.init.clone());

                    let ret = tgr.tag(el.map(WindowItem::strip));
                    self.output_buffer.extend(
                        ret.into_iter()
                            .map(|el| WindowItem::from_parts(key.clone(), tag.clone(), el))
                            .map(StreamElement::Item),
                    );
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                el => {
                    self.tagger.windows.retain(|kt, tgr| {
                        let ret = tgr.tag(el.clone().map(WindowItem::strip));
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|el| WindowItem::from_parts(kt.0.clone(), kt.1.clone(), el))
                                .map(StreamElement::Item),
                        );
                        !tgr.recycle()
                    });

                    // Forward system messages and watermarks
                    let msg = match el {
                        StreamElement::Watermark(w) => StreamElement::Watermark(w),
                        StreamElement::Terminate => StreamElement::Terminate,
                        StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
                        _ => unreachable!(),
                    };
                    self.output_buffer.push_back(msg);
                }
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(
                OperatorStructure::new::<WindowItem<K, ChainTag<NoTag>, I>, _>(&self.name),
            )
    }
}
