use std::collections::HashMap;
use std::fmt::Display;

use crate::block::OperatorStructure;
use crate::operator::streaming_window::streamingwindow::WindowMsg;
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Count<Op, I>
where
    Op: Operator<Out = (((), u64), WindowMsg<I>)>,
    I: Data,
{
    prev: Op,
    window_counts: HashMap<((), u64), (usize, Option<Timestamp>)>,
}

impl<Op, I> Display for Count<Op, I>
where
    Op: Operator<Out = (((), u64), WindowMsg<I>)>,
    I: Data,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Count<{}>", self.prev, std::any::type_name::<I>(),)
    }
}

impl<Op, I> Count<Op, I>
where
    Op: Operator<Out = (((), u64), WindowMsg<I>)>,
    I: Data,
{
    pub(crate) fn new(prev: Op) -> Self {
        Count {
            prev,
            window_counts: HashMap::new(),
        }
    }
}

impl<Op, I> Operator for Count<Op, I>
where
    Op: Operator<Out = (((), u64), WindowMsg<I>)>,
    I: Data,
{
    type Out = ((), usize);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> crate::operator::StreamElement<Self::Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item((path, WindowMsg::Item(_))) => {
                    let (window_count, _) =
                        self.window_counts.entry(path).or_insert_with(|| (0, None));
                    *window_count += 1;
                }
                StreamElement::Timestamped((path, WindowMsg::Item(_)), ts) => {
                    let (window_count, t) =
                        self.window_counts.entry(path).or_insert_with(|| (0, None));
                    *t = Some(t.unwrap_or(ts).max(ts));
                    *window_count += 1;
                }
                StreamElement::Item((id, WindowMsg::WindowClosed))
                | StreamElement::Timestamped((id, WindowMsg::WindowClosed), _) => {
                    let (final_count, ts) = self.window_counts.remove(&id).unwrap_or((0, None));
                    let elem = (id.0, final_count);
                    return match ts {
                        Some(ts) => StreamElement::Timestamped(elem, ts),
                        None => StreamElement::Item(elem),
                    };
                }
                StreamElement::Watermark(wm) => return StreamElement::Watermark(wm),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }
    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<usize, _>("StreamingWindow_Count"))
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct NestedCount<Op, I, P>
where
    Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
    I: Data,
    P: DataKey,
{
    prev: Op,
    window_counts: HashMap<(P, u64), (usize, Option<Timestamp>)>,
}

impl<Op, I, P> Display for NestedCount<Op, I, P>
where
    Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
    I: Data,
    P: DataKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Count<{}>", self.prev, std::any::type_name::<I>(),)
    }
}

impl<Op, I, P> NestedCount<Op, I, P>
where
    P: DataKey,
    Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
    I: Data,
{
    pub(crate) fn new(prev: Op) -> Self {
        NestedCount {
            prev,
            window_counts: HashMap::new(),
        }
    }
}

impl<Op, I, P> Operator for NestedCount<Op, I, P>
where
    Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
    I: Data,
    P: DataKey,
{
    type Out = (P, WindowMsg<usize>);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> crate::operator::StreamElement<Self::Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item((path, WindowMsg::Item(_))) => {
                    let (window_count, _) =
                        self.window_counts.entry(path).or_insert_with(|| (0, None));
                    *window_count += 1;
                }
                StreamElement::Timestamped((path, WindowMsg::Item(_)), ts) => {
                    let (window_count, t) =
                        self.window_counts.entry(path).or_insert_with(|| (0, None));
                    *t = Some(t.unwrap_or(ts).max(ts));
                    *window_count += 1;
                }
                StreamElement::Item((id, WindowMsg::WindowClosed))
                | StreamElement::Timestamped((id, WindowMsg::WindowClosed), _) => {
                    let (final_count, ts) = self.window_counts.remove(&id).unwrap_or((0, None));
                    let elem = (id.0, WindowMsg::Item(final_count));
                    return match ts {
                        Some(ts) => StreamElement::Timestamped(elem, ts),
                        None => StreamElement::Item(elem),
                    };
                }
                StreamElement::Watermark(wm) => return StreamElement::Watermark(wm),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }
    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<WindowMsg<usize>, _>(
                "StreamingWindow_NestedCount",
            ))
    }
}
