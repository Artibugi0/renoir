use std::fmt::Display;

use crate::{ExecutionMetadata, block::{BlockStructure, OperatorStructure}, operator::{Data, Operator, StreamElement, streaming_window::streamingwindow::WindowMsg}};

#[derive(Clone)]
pub(crate) struct UnwrapMsg<Op, I>
where
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    I: Data,
{
    prev: Op,
}

impl<Op, I> UnwrapMsg<Op, I>
where
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    I: Data,
{
    pub(crate) fn new(prev: Op) -> Self {
        Self { prev }
    }
}

impl<Op, I> Display for UnwrapMsg<Op, I>
where
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    I: Data,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> UnwrapMsg<{}>", self.prev, std::any::type_name::<I>())
    }
}

impl<Op, I> Operator for UnwrapMsg<Op, I>
where
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    I: Data,
{
    type Out = (u64, I);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(u64, I)> {
        loop {
            match self.prev.next() {
                StreamElement::Item((id, WindowMsg::Item(x))) => {
                    return StreamElement::Item((id, x))
                }
                StreamElement::Timestamped((id, WindowMsg::Item(x)), ts) => {
                    return StreamElement::Timestamped((id, x), ts)
                }
                // marcatori: per ora scartati
                StreamElement::Item((_, WindowMsg::WindowClosed))
                | StreamElement::Timestamped((_, WindowMsg::WindowClosed), _) => {}
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::Terminate => return StreamElement::Terminate,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<(u64, I), _>("UnwrapMsg"))
    }
}
