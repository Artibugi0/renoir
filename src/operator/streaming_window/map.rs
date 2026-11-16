use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow:: WindowMsg;
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Map<P,I,O, F, Op>
where
    O: Send,
    F: Fn(I) -> O + Send + Clone,
    Op: Operator<Out = (P,WindowMsg<I>)>,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    f: F
}

impl<P,I,O, F, Op> Clone for Map<P,I,O, F, Op>
where
    O: Send,
    F: Fn(I) -> O + Send + Clone,
    Op: Operator<Out = (P,WindowMsg<I>)>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
        }
    }
}

impl<P,I,O, F, Op> Display for Map<P,I,O, F, Op>
where
    O: Send,
    F: Fn(I) -> O + Send + Clone,
    Op: Operator<Out = (P,WindowMsg<I>)>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Map<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<O>()
        )
    }
}

impl<P,I,O, F, Op> Map<P,I,O, F, Op>
where
    P: DataKey,
    I: Data,
    O: Data,
    F: Fn(I) -> O + Send + Clone,
    Op: Operator<Out = (P,WindowMsg<I>)>,
{
    pub(super) fn new(prev: Op, f: F) -> Self {
        Self { prev, f }
    }
}

impl<P,I,O, F, Op> Operator for Map<P,I,O, F, Op>
where
    P: DataKey,
    I: Data,
    O: Data,
    F: Fn(I) -> O + Send + Clone,
    Op: Operator<Out = (P,WindowMsg<I>)>,
{
    type Out = (P,WindowMsg<O>);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Self::Out> {
        self.prev.next().map_window_msg(&self.f)
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>("Streaming_Window_Map"))
    }
}

impl<I> WindowMsg<I> {
    pub fn map<O>(self, f: impl FnOnce(I) -> O) -> WindowMsg<O> {
        match self {
            WindowMsg::Item(i) => WindowMsg::Item(f(i)),
            WindowMsg::WindowClosed => WindowMsg::WindowClosed,
        }
    }
}

impl<P, I> StreamElement<(P, WindowMsg<I>)> {
    pub fn map_window_msg<O>(self, f: impl FnOnce(I) -> O) -> StreamElement<(P, WindowMsg<O>)> {
        match self {
            StreamElement::Item((p, m)) => StreamElement::Item((p, m.map(f))),
            StreamElement::Timestamped((p, m), ts) => StreamElement::Timestamped((p, m.map(f)), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }
}



   


