use std::collections::HashMap;
use std::fmt::Display;



use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::WindowMsg;
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<O: Send + Clone, F, Op,I>
where
    F: Fn(&mut O,I) + Send + Clone,
    Op: Operator<Out = (u64,WindowMsg<I>)>,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: O,
    accumulators: HashMap<u64,O>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<O: Send + Clone, F, Op,I> Display for Fold<O, F,Op,I>
where
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = (u64, WindowMsg<I>)>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Fold<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<O: Send + Clone, F, Op: Operator,I> Fold<O, F, Op,I>
where
    F: Fn(&mut O,I) + Send + Clone,
    Op: Operator<Out = (u64,WindowMsg<I>)>
{
    pub(super) fn new(prev: Op, init: O, fold: F) -> Self {
        Fold {
            prev,
            fold,
            init,
            accumulators: HashMap::new(),
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<O: Send + Clone, F, Op,I> Operator for Fold<O, F, Op,I>
where
    F: Fn(&mut O,I) + Send + Clone,
    Op: Operator<Out = (u64, WindowMsg<I>)>,
    I: Data,
{
    type Out = (u64,O);
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
       self.prev.setup(metadata); 
    }

    fn next(&mut self) -> StreamElement<(u64,O)> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Item((id, WindowMsg::Item(x))) => {
                    let acc = 
                        self.accumulators
                        .entry(id)
                        .or_insert(self.init.clone());
                    (self.fold)(acc,x); 
                }
                StreamElement::Timestamped((id, WindowMsg::Item(x)), ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    let acc = 
                        self.accumulators
                        .entry(id)
                        .or_insert(self.init.clone());
                    (self.fold)(acc,x); 
                }
                StreamElement::Item((id,WindowMsg::WindowClosed))  => {
                    if let Some(acc) = self.accumulators.remove(&id){
                        return StreamElement::Item((id,acc))
                    }
                   
                }
                StreamElement::Timestamped((id,WindowMsg::WindowClosed),ts ) =>{
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if let Some(acc) = self.accumulators.remove(&id){
                        if let Some(ts) = self.timestamp.take() {
                            return StreamElement::Timestamped((id,acc), ts);
                        }
                    }
                }               
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        // If there is an accumulated value, return it
        /*if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(acc, ts);
            } else {
                return StreamElement::Item(acc);
            }
        }*/

        // If watermark were received, send one downstream
        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("Streaming_Window_Fold"))
    }
}
