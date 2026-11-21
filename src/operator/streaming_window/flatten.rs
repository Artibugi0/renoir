use std::fmt::Display;

#[cfg(feature = "timestamp")]
use crate::operator::{StreamElement, Timestamp};
use crate::{
    block::OperatorStructure,
    operator::{streaming_window::streamingwindow::WindowMsg, Data, DataKey, Operator},
};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Flatten<P, I, Op>
where
    Op: Operator<Out = (P, WindowMsg<I>)>,
    I: IntoIterator,
    I::Item: Send,
    I::IntoIter: Send,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    original_iter: Option<(P, I::IntoIter)>,
    #[cfg(feature = "timestamp")]
    timestamp: Option<Timestamp>,
}

impl<P: Clone, I, Op: Clone> Clone for Flatten<P, I, Op>
where
    Op: Operator<Out = (P, WindowMsg<I>)>,
    I: IntoIterator,
    I::Item: Send,
    I::IntoIter: Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            original_iter: Default::default(),
            timestamp: Default::default(),
        }
    }
}

impl<P, I, Op> Display for Flatten<P, I, Op>
where
    Op: Operator<Out = (P, WindowMsg<I>)>,
    I: IntoIterator,
    I::Item: Send,
    I::IntoIter: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Flatten<{}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            //std::any::type_name::<(P,<Op::Out as IntoIterator>::Item)>()
        )
    }
}

impl<P, I, Op> Flatten<P, I, Op>
where
    P: DataKey,
    I: Data + IntoIterator,
    I::Item: Data,
    I::IntoIter: Send,
    Op: Operator<Out = (P, WindowMsg<I>)>,
{
    pub(super) fn new(prev: Op) -> Self {
        Self {
            prev,
            original_iter: None,
            #[cfg(feature = "timestamp")]
            timestamp: None,
        }
    }
}

impl<P, I, Op> Operator for Flatten<P, I, Op>
where
    P: DataKey,
    I: Data + IntoIterator,
    I::Item: Data,
    I::IntoIter: Send,
    Op: Operator<Out = (P, WindowMsg<I>)>,
{
    type Out = (P, WindowMsg<I::Item>);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        loop {
            if let Some((ref key, ref mut inner)) = self.original_iter {
                match inner.next() {
                    None => self.original_iter = None,
                    #[cfg(not(feature = "timestamp"))]
                    Some(item) => return StreamElement::Item((key.clone(), item)),
                    #[cfg(feature = "timestamp")]
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
                #[cfg(not(feature = "timestamp"))]
                StreamElement::Item((key, inner)) | StreamElement::Timestamped((key, inner), _) => {
                    self.original_iter = Some((key, inner.into_iter()));
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Item((p, WindowMsg::Item(item))) => {
                    self.original_iter = Some((p, item.into_iter()));
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Item((p, WindowMsg::WindowClosed)) => {
                    return StreamElement::Item((p, WindowMsg::WindowClosed));
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped((p, WindowMsg::WindowClosed), ts) => {
                    return StreamElement::Timestamped((p, WindowMsg::WindowClosed), ts);
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped((p, WindowMsg::Item(item)), ts) => {
                    self.original_iter = Some((p, item.into_iter()));
                    self.timestamp = Some(ts);
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>(
                "StreamingWindow_Flatten",
            ))
    }
}
