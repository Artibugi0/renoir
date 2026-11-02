use crate::{ WindowedStream,operator::{Data, DataKey, Operator,streaming_window::streamingwindow::{FirstClassWindowedStream, WindowMsg}, window::{WindowAccumulator, WindowDescription}}};
use std::fmt::Debug;
use flume::Sender;
use crate::block::Replication;
use crate::operator::source::ChannelSource;
use crate::stream::{Stream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;


//provvisorio,probabilmente la dimensione del canale sarà pari 
//alle dimensioni fornite alla finestra nel momento della sua costruzione
const CHANNEL_SIZE: usize = 1024; 

pub struct WindowS<I: Send + 'static> {
    
    //ogni clone ha un sender che invia tutto allo stesso blocco
    sender: Sender<(u64, WindowMsg<I>)>,    
    
    next_id: Arc<AtomicU64>, // condiviso da tutti i cloni
    
    // id di questa finestra
    id: u64,                 
}

impl<I: Send + 'static> WindowS<I> {
    pub fn new(sender: Sender<(u64, WindowMsg<I>)>) -> Self {
        Self { sender, next_id: Arc::new(AtomicU64::new(1)), id: 0 }
    }
}

impl<I: Send + 'static> Clone for WindowS<I> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            next_id: self.next_id.clone(),
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl<I> WindowAccumulator for WindowS<I>
where
    I: Data + Send + Clone
{
    type In = I;
    type Out = ();

    #[inline]
    fn process(&mut self, el: &Self::In) {
        //capire come gestire le chiavi
        self.sender.send((self.id, WindowMsg::Item(el.clone()))).unwrap();
    }

    fn output(self) {
        self.sender.send((self.id,WindowMsg::WindowClosed)).unwrap();
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out> + 'static,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data + Debug,
{
    pub fn to_streaming_window(
        self,
    ) -> FirstClassWindowedStream<impl Operator<Out = (u64, WindowMsg<Out>)>> {
        //Crea canale per poter inviare gli elementi al nuovo blocco
        let (tx, source) = 
            ChannelSource::<(u64, WindowMsg<Out>)>::new(CHANNEL_SIZE, Replication::One);

        let ctx = self.inner.0.ctx.clone();
        
        //Chiudi blocco corrente con for_each che scarta l'output vuoto del nostro operatore (Key,())
        self.add_window_operator("to_streaming_window", WindowS::new(tx))
            .for_each(|_| {});
        
        
        let block = ctx
            .lock()
            .new_block(source, Default::default(), Default::default());
        let downstream = Stream::new(ctx, block);

        FirstClassWindowedStream(downstream)
    }
}
