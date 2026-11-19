use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::{KWElement, WindowMsg};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

//variante di filter per StreamingWindow, adattata per ricevere gli elementi della enum WindowMsg
//cambiano i generici utilizzati,manca qualcosa/va modificato?
#[derive(Clone)]
pub struct Filter<Op, F, K, I>
where
    Op: Operator<Out = KWElement<K, I>> + 'static,
    F: Fn(&I) -> bool + Clone + Send + 'static,
{
    prev: Op,
    predicate: F,
    marker: PhantomData<(K, I)>,
}

impl<Op, F, K, I> Display for Filter<Op, F, K, I>
where
    Op: Operator<Out = KWElement<K, I>> + 'static,
    F: Fn(&I) -> bool + Clone + Send + 'static,
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

impl<Op, F, K, I> Filter<Op, F, K, I>
where
    I: Send,
    Op: Operator<Out = KWElement<K, I>> + 'static,
    F: Fn(&I) -> bool + Clone + Send + 'static,
{
    pub(super) fn new(prev: Op, predicate: F) -> Self {
        Self {
            prev,
            predicate,
            marker: PhantomData,
        }
    }
}

impl<Op, F, K, I> Operator for Filter<Op, F, K, I>
where
    I: Send + Clone,
    K: Send + Clone,
    Op: Operator<Out = KWElement<K, I>> + 'static,
    F: Fn(&I) -> bool + Clone + Send + 'static,
{
    type Out = Op::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Op::Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item((_, _, WindowMsg::Item(ref item)))
                | StreamElement::Timestamped((_, _, WindowMsg::Item(ref item)), _)
                    if !(self.predicate)(item) => {} // scarta
                element => return element,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Op::Out, _>(
                "Streaming_Window_Filter",
            ))
    }
}
