use std::collections::HashMap;
use std::fmt::Display;



use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::WindowMsg;
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

//Questa implementazione di fold mantiene una hashmap di accumulatori
//ognuno associato ad una particolare finestra
//il resto dell'implementazione è simile a quella usata per stream normali
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
    //hashmap con chiave id della finestra
    accumulators: HashMap<u64,(O, Option<Timestamp>)>,
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

                //match si aspetta elementi della finestra, accumulatori creati/aggiornati in base all'id della finestra
                //associato all'elemento
                StreamElement::Item((id, WindowMsg::Item(x))) => {
                    let (acc,_) = 
                        self.accumulators
                        .entry(id)
                        .or_insert_with(|| (self.init.clone(),None));
                    (self.fold)(acc,x); 
                }
                //timestamp gestiti come nell'implementazione di base
                StreamElement::Timestamped((id, WindowMsg::Item(x)), ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    let (acc,t) = 
                        self.accumulators
                        .entry(id)
                        .or_insert_with(|| (self.init.clone(),None));
                    *t = Some(t.unwrap_or(ts).max(ts));
                    (self.fold)(acc,x); 
                }
                //se ricevo windowclosed so che teoricamente elementi della finestra sono esauriti
                //sputo fuori l'accumulatore completato e cancello l'entry dalla mappa
                StreamElement::Item((id, WindowMsg::WindowClosed))
                | StreamElement::Timestamped((id, WindowMsg::WindowClosed), _) => {
                if let Some((acc, ts)) = self.accumulators.remove(&id) {
                    return match ts {
                        Some(ts) => StreamElement::Timestamped((id, acc), ts),
                        None => StreamElement::Item((id, acc)),
                    };
                }
            }                
                //logica invariata     

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
