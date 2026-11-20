use std::collections::VecDeque;
use std::fmt::Display;


use crate::block::OperatorStructure;
use crate::operator::streaming_window::streamingwindow::{WindowMsg, WindowTag};
// use crate::operator::streaming_window::window_operators::fold::NestedFold;
use crate::operator::window::{KeyedWindowTagger, WindowTagger};
use crate::operator::{Data, DataKey, StreamElement};

use crate::operator::Operator;
/// This operator abstracts the window logic as an operator and delegates to the
/// `KeyedWindowManager` and a `ProcessFunc` the job of building and processing the windows,
/// respectively.
#[derive(Clone)]
pub(crate) struct WindowRouter<K, I, Op, W>
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
    tagger: KeyedWindowTagger<K, I, W>,
    /// A buffer for storing ready items.
    output_buffer: VecDeque<StreamElement<(K, WindowTag, WindowMsg<I>)>>,
}

impl<K, I, Prev, W> WindowRouter<K, I, Prev, W>
where
    W: WindowTagger,
{
    pub(crate) fn new(prev: Prev, name: &str, tagger: KeyedWindowTagger<K, I, W>) -> Self {
        Self {
            prev,
            name: name.into(),
            tagger,
            output_buffer: Default::default(),
        }
    }
}

impl<K, I, Prev, W> Display for WindowRouter<K, I, Prev, W>
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
            std::any::type_name::<WindowMsg<I>>(),
        )
    }
}

impl<K, I, Prev, W> Operator for WindowRouter<K, I, Prev, W>
where
    W: WindowTagger + Send,
    Prev: Operator<Out = (K, I)>,
    K: DataKey,
    I: Data,
{
    type Out = (K, WindowTag, WindowMsg<I>);

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
                        .entry(key.clone())
                        .or_insert_with(|| self.tagger.init.clone());

                    let ret = tgr.map(el);
                    self.output_buffer.extend(
                        ret.into_iter()
                            .map(|e| StreamElement::Item((key.clone(), e.tag, e.item))),
                    );
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                el => {
                    let (_, el) = el.take_key();

                    self.tagger.windows.retain(|key, mgr| {
                        let ret = mgr.map(el.clone());
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|e| StreamElement::Item((key.clone(), e.tag, e.item))),
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
            .add_operator(OperatorStructure::new::<(K, WindowMsg<I>), _>(&self.name))
    }
}
