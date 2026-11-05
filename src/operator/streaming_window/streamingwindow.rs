use crate::KeyedStream;
use crate::operator::Data;
use crate::operator::streaming_window::filter::Filter;
use crate::operator::streaming_window::fold::Fold;
use crate::stream::Stream;

use crate::{operator::Operator, stream::KeyedItem};

// proposta di enum per classificare tipi di dati provenienti dalla finestra
//Item wrappa ogni elemento ricevuto dall'accumulatore prima di inviarlo al nuovo blocco
//WindowClosed indica la fine degli elementi ricevuti dall'operatore 
//da capire come gestire finestre a più livelli
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
    //andrà modificata in futuro
    //ancora non è chiaro al 100% come uscire dal contesto di StreamingWindow 
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
}

impl<Op, I> FirstClassWindowedStream<Op>
where
    Op: Operator<Out = (u64, WindowMsg<I>)> + 'static,
    I: Data,
{
    //fold è un'operazione di riduzione, dunque usciamo dal contesto di streamingwindow, non abbiamo più gli elementi della finestra
    //ma un loro risultato, dunque usciamo dal contesto
    //SICURAMENTE, il comportamento andrà cambiato durante l'implementazione delle finestre a multipli livelli
    pub fn fold<O,F>(self,init:O,f:F) -> KeyedStream<impl Operator<Out = (u64,O)>>
    where 
        O : Data + Clone,
        F: Fn(&mut O,I) + Send + Clone + 'static,
        {
            KeyedStream(self.0.add_operator(|prev|Fold::new(prev,init,f)))
        }  
    
    
    //filter è un'operazione non di riduzione ma di modifica degli elementi della finestra, dunque rimaniamo nel contesto
    pub fn filter<F>(self,predicate: F) -> FirstClassWindowedStream<impl Operator<Out = (u64,WindowMsg<I>)>>
    where
        F: Fn(&I)-> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| Filter::new(prev, predicate))
    }
}
