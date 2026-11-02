use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::WindowMsg;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
pub struct Filter<Op, Predicate,I>
where
    I: Send,
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    Predicate: Fn(&I) -> bool + Clone + Send + 'static,
{
    prev: Op,
    predicate: Predicate,
}

impl<Op, Predicate,I> Display for Filter<Op, Predicate,I>
where
    I: Send,
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    Predicate: Fn(&I) -> bool + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Filter<{}>",
            self.prev,
            std::any::type_name::<Op::Out>()
        )
    }
}

impl<Op, Predicate,I> Filter<Op, Predicate,I>
where
    I: Send,
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    Predicate: Fn(&I) -> bool + Clone + Send + 'static,
{
    pub(super) fn new(prev: Op, predicate: Predicate) -> Self {
        Self { prev, predicate }
    }
}


impl<Op, Predicate,I> Operator for Filter<Op, Predicate,I>
where
    I: Send + Clone,
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    Predicate: Fn(&I) -> bool + Clone + Send + 'static,
    
{
    type Out = Op::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Op::Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item((_, WindowMsg::Item(ref item)))
                | StreamElement::Timestamped((_, WindowMsg::Item(ref item)), _)
                    if !(self.predicate)(item) => {} // scarta
                element => return element,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Op::Out, _>("Streaming_Window_Filter"))
    }
}
