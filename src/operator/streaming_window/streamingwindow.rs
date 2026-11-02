use crate::KeyedStream;
use crate::operator::Data;
use crate::operator::streaming_window::fold::Fold;
use crate::stream::Stream;

use crate::{operator::{Operator, Filter}, stream::KeyedItem};

// proposta di enum per classificare tipi di dati provenienti dalla finestra
//Item wrappa ogni elemento ricevuto dall'accumulatore prima di inviarlo al nuovo blocco
//WindowClosed indica la fine degli elementi ricevuti dall'operatore 
#[derive(Clone, Debug)]
pub enum WindowMsg<I> {
    Item(I),
    WindowClosed,
}

pub struct FirstClassWindowedStream<OperatorChain>(pub Stream<OperatorChain>) 
where
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem;

impl <OperatorChain> FirstClassWindowedStream<OperatorChain> where 
    OperatorChain: Operator + 'static,
    OperatorChain::Out: KeyedItem,
{
    pub fn close(self) -> KeyedStream<OperatorChain> {
        
        KeyedStream(self.0)
    }

    pub(crate) fn add_operator<Op2, GetOp>(self, get_operator: GetOp) -> FirstClassWindowedStream<Op2>
    where
        Op2: Operator,
        GetOp: FnOnce(OperatorChain) -> Op2,
        Op2::Out: KeyedItem<Key = <OperatorChain::Out as KeyedItem>::Key>,
    {
        FirstClassWindowedStream(self.0.add_operator(get_operator))
    }
    
    pub fn filter<F>(self,predicate: F,) -> FirstClassWindowedStream<impl Operator<Out = OperatorChain::Out>>
    where
        F: Fn(&<OperatorChain::Out as KeyedItem>::Value) -> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| {
            Filter::new(prev, move |item: &OperatorChain::Out| predicate(item.value()))
        })
    }
    
  
    
}

impl<Op, I> FirstClassWindowedStream<Op>
where
    Op: Operator<Out = (u64, WindowMsg<I>)> + 'static,
    I: Data,
{
        pub fn fold<O,F>(self,init:O,f:F) -> Stream<impl Operator<Out = (u64,O)>>
    where 
        O : Data + Clone,
        F: Fn(&mut O,I) + Send + Clone + 'static,
        {
            self.0.add_operator(|prev|Fold::new(prev,init,f))
        }  
}
