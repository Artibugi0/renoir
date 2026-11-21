use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::WindowMsg;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

//variante di filter per StreamingWindow, adattata per ricevere gli elementi della enum WindowMsg
//cambiano i generici utilizzati,manca qualcosa/va modificato?
#[derive(Clone)]
pub struct Filter<Op, Predicate, K, I>
where
    Op: Operator<Out = (K, WindowMsg<I>)>,
    Predicate: Fn(&I) -> bool + Clone + Send + 'static,
{
    prev: Op,
    predicate: Predicate,
    marker: PhantomData<(K, I)>,
}

impl<Op, Predicate, K, I> Display for Filter<Op, Predicate, K, I>
where
    Op: Operator<Out = (K, WindowMsg<I>)>,
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

impl<Op, Predicate, K, I> Filter<Op, Predicate, K, I>
where
    I: Send,
    Op: Operator<Out = (K, WindowMsg<I>)>,
    Predicate: Fn(&I) -> bool + Clone + Send + 'static,
{
    pub(super) fn new(prev: Op, predicate: Predicate) -> Self {
        Self {
            prev,
            predicate,
            marker: PhantomData,
        }
    }
}

impl<Op, Predicate, K, I> Operator for Filter<Op, Predicate, K, I>
where
    I: Send + Clone,
    K: Send + Clone,
    Op: Operator<Out = (K, WindowMsg<I>)>,
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
            .add_operator(OperatorStructure::new::<Op::Out, _>(
                "StreamingWindow_Filter",
            ))
    }
}
