use crate::block::Replication;
use crate::operator::source::ChannelSource;
use crate::stream::Stream;
use crate::{
    operator::{
        streaming_window::streamingwindow::{
            Depth, FirstClassWindowedStream, Level, WindowMsg, Zero,
        },
        window::{WindowAccumulator, WindowDescription},
        Data, DataKey, Operator,
    },
    stream::KeyedItem,
    WindowedStream,
};
use flume::Sender;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

//provvisorio,probabilmente la dimensione del canale sarà pari
//alle dimensioni fornite alla finestra nel momento della sua costruzione
const CHANNEL_SIZE: usize = 1024;

pub struct WindowS<I: Send + 'static, D: Depth> {
    //ogni clone ha un sender che invia tutto allo stesso blocco
    sender: Sender<((D::Path, u64), WindowMsg<I>)>,

    window_path: D::Path,

    next_id: Arc<AtomicU64>, // condiviso da tutti i cloni

    // id di questa finestra
    id: u64,
}

impl<I: Send + 'static, D: Depth> WindowS<I, D> {
    pub fn new(sender: Sender<((D::Path, u64), WindowMsg<I>)>, window_path: D::Path) -> Self {
        Self {
            sender,
            window_path,
            next_id: Arc::new(AtomicU64::new(1)),
            id: 0,
        }
    }
}

impl<I: Send + 'static, D: Depth> Clone for WindowS<I, D> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            window_path: self.window_path.clone(),
            next_id: self.next_id.clone(),
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl<I, D> WindowAccumulator for WindowS<I, D>
where
    I: Data,
    D: Depth + 'static,
{
    type In = I;
    type Out = ();

    #[inline]
    fn process(&mut self, el: &Self::In) {
        let key = (self.window_path.clone(), self.id);
        self.sender
            .send((key, WindowMsg::Item(el.clone())))
            .unwrap();
    }

    fn output(self) {
        let key = (self.window_path.clone(), self.id);
        self.sender.send((key, WindowMsg::WindowClosed)).unwrap();
    }
}

//versione per entrare nel contesto di StreamingWindow
impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out> + 'static,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data + Debug,
{
    pub fn to_streaming_window(
        self,
    ) -> FirstClassWindowedStream<
        impl Operator<Out = (<Level<Zero> as Depth>::Path, WindowMsg<Out>)>,
        Level<Zero>,
    > {
        //Crea canale per poter inviare gli elementi al nuovo blocco
        let (tx, source) = ChannelSource::<(<Level<Zero> as Depth>::Path, WindowMsg<Out>)>::new(
            CHANNEL_SIZE,
            Replication::One,
        );

        let ctx = self.inner.0.ctx.clone();

        //Chiudi blocco corrente con for_each che scarta l'output vuoto del nostro operatore (Key,())
        self.add_window_operator("to_streaming_window", WindowS::<Out, Zero>::new(tx, ()))
            .for_each(|_| {});

        let block = ctx
            .lock()
            .new_block(source, Default::default(), Default::default());
        let downstream = Stream::new(ctx, block);

        FirstClassWindowedStream::new(downstream)
    }
}

/*
//Versione per aumentare la profondità di un livello
//ha ancora bisogno di essere completata

pub struct NestedWindowS<I:Send + 'static,D:Depth> {
    //ogni clone ha un sender che invia tutto allo stesso blocco
    sender: Sender<((D::Path,u64), WindowMsg<I>)>,

    next_id: Arc<AtomicU64>, // condiviso da tutti i cloni

    // id di questa finestra
    id: u64,
    key: Option<D::Path>
}
impl<I: Send + 'static, D:Depth> NestedWindowS<I,D> {
    pub fn new(sender: Sender<((D::Path,u64), WindowMsg<I>)>) -> Self {
        Self { sender, next_id: Arc::new(AtomicU64::new(1)), id: 0, key: None }
    }
}

impl<I: Send + 'static, D: Depth> Clone for NestedWindowS<I,D> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            next_id: self.next_id.clone(),
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            key: self.key.clone()
        }
    }
}
impl<I,D> WindowAccumulator for NestedWindowS<I,D>
where
    I: Data,
    D: Depth + 'static
{
    type In = (D::Path,I);
    type Out = ();

    #[inline]
    fn process(&mut self, el: &Self::In) {

        if self.key.is_none() {
            self.key = Some(el.key().clone());
        }
        let key = (el.key().clone(), self.id);
        self.sender.send((key, WindowMsg::Item(el.value().clone()))).unwrap();
        }

    fn output(self) {
        let key = (self.key.unwrap(), self.id);
        self.sender.send((key,WindowMsg::WindowClosed)).unwrap();
    }
}

impl<Op, WinOut, WinDescr, D,I> FirstClassWindowedWithDescr<Op,WinOut, WinDescr,Level<D>>
where
    WinDescr: WindowDescription<I> + 'static,
    D:Depth + Debug,
    Op: Operator<Out = (<Level<D> as Depth>::Path, WindowMsg<I>)> + 'static,
    WinOut: 'static,
    I: Data + Debug,
{
    pub fn to_streaming_window(self) -> FirstClassWindowedStream<
    impl Operator<Out = (<Level<Level<D>> as Depth>::Path, WindowMsg<I>)>,
    Level<Level<D>>> {
        //Crea canale per poter inviare gli elementi al nuovo blocco
        let (tx, source) = ChannelSource::<((<Level<D> as Depth>::Path,u64), WindowMsg<I>)>::new(CHANNEL_SIZE, Replication::One);
        let ctx = self.inner.0.ctx.clone();

        //Chiudi blocco corrente con for_each che scarta l'output vuoto del nostro operatore (Key,())
        self.add_window_operator("to_streaming_window", NestedWindowS::new(tx))
            .for_each(|_| {});


        let block = ctx
            .lock()
            .new_block(source, Default::default(), Default::default());
        let downstream = Stream::new(ctx, block);

        FirstClassWindowedStream::new(downstream)
    }
}*/
