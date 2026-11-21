use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::WindowMsg;
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use core::iter::{IntoIterator, Iterator};
use std::fmt::Display;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct FlatMap<It, I, F, Op, P>
where
    P: DataKey,
    I: Data,
    Op: Operator<Out = (P, WindowMsg<I>)>,
    It: IntoIterator,
    It::IntoIter: Send,
    It::Item: Data,
    F: Fn(I) -> It + Clone + Send,
{
    prev: Op,
    f: F,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `FlatMap` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<(P, It::IntoIter)>,
    #[cfg(feature = "timestamp")]
    timestamp: Option<Timestamp>,
}

impl<It, I, F, Op, P> Clone for FlatMap<It, I, F, Op, P>
where
    P: DataKey,
    I: Data,
    Op: Operator<Out = (P, WindowMsg<I>)>,
    It: IntoIterator,
    It::IntoIter: Send,
    It::Item: Data,
    F: Fn(I) -> It + Clone + Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
            frontiter: None,
            #[cfg(feature = "timestamp")]
            timestamp: self.timestamp,
        }
    }
}

impl<It, I, F, Op, P> Display for FlatMap<It, I, F, Op, P>
where
    P: DataKey,
    I: Data,
    Op: Operator<Out = (P, WindowMsg<I>)>,
    It: IntoIterator,
    It::IntoIter: Send,
    It::Item: Data,
    F: Fn(I) -> It + Clone + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FlatMap<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<It::Item>()
        )
    }
}

impl<It, I, F, Op, P> FlatMap<It, I, F, Op, P>
where
    P: DataKey,
    I: Data,
    Op: Operator<Out = (P, WindowMsg<I>)>,
    It: IntoIterator,
    It::IntoIter: Send,
    It::Item: Data,
    F: Fn(I) -> It + Clone + Send,
{
    pub(super) fn new(prev: Op, f: F) -> Self {
        Self {
            prev,
            f,
            frontiter: None,
            #[cfg(feature = "timestamp")]
            timestamp: None,
        }
    }
}

impl<It, I, F, Op, P> Operator for FlatMap<It, I, F, Op, P>
where
    P: DataKey,
    I: Data,
    Op: Operator<Out = (P, WindowMsg<I>)>,
    It: IntoIterator,
    It::IntoIter: Send,
    It::Item: Data,
    F: Fn(I) -> It + Clone + Send,
{
    type Out = (P, WindowMsg<It::Item>);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        loop {
            if let Some((ref key, ref mut inner)) = self.frontiter {
                match inner.next() {
                    None => self.frontiter = None,
                    Some(item) => {
                        let out = (key.clone(), WindowMsg::Item(item));
                        match self.timestamp {
                            None => return StreamElement::Item(out),
                            Some(ts) => return StreamElement::Timestamped(out, ts),
                        }
                    }
                }
            }
            match self.prev.next() {
                StreamElement::Item((key, msg)) => {
                    self.timestamp = None;
                    match msg {
                        WindowMsg::Item(v) => {
                            self.frontiter = Some((key, (self.f)(v).into_iter()));
                        }
                        WindowMsg::WindowClosed => {
                            return StreamElement::Item((key, WindowMsg::WindowClosed));
                        }
                    }
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped((key, msg), ts) => {
                    self.timestamp = Some(ts);
                    match msg {
                        WindowMsg::Item(v) => {
                            self.frontiter = Some((key, (self.f)(v).into_iter()));
                        }
                        WindowMsg::WindowClosed => {
                            return StreamElement::Timestamped((key, WindowMsg::WindowClosed), ts);
                        }
                    }
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<It::Item, _>(
                "StreamingWindow_FlatMap",
            ))
    }
}
