use crate::{
    block::OperatorStructure,
    operator::{
        streaming_window::streamingwindow::{
            Depth, FirstClassWindowedStream, FirstClassWindowedWithDescr, Level, WindowElement,
            WindowMsg, WindowTag, Zero,
        },
        window::{
            KeyedWindowTagger, StreamingWindowDescription, WindowAccumulator, WindowDescription,
            WindowManager, WindowTagger,
        },
        Data, DataKey, Operator, StreamElement,
    },
    WindowedStream,
};
use flume::Sender;
use nanorand::{Rng, WyRand};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    marker::PhantomData,
};

//provvisorio,probabilmente la dimensione del canale sarà pari
//alle dimensioni fornite alla finestra nel momento della sua costruzione
const CHANNEL_SIZE: usize = 1024;

// pub struct WindowS<I: Send + 'static, D: Depth> {

//     //ogni clone ha un sender che invia tutto allo stesso blocco
//     sender: Sender<((D::Path,u64), WindowMsg<I>)>,

//     window_path: D::Path,

//     next_id: Arc<AtomicU64>, // condiviso da tutti i cloni

//     // id di questa finestra
//     id: WindowTag,
// }

// impl<I: Send + 'static, D:Depth> WindowS<I,D> {
//     pub fn new(sender: Sender<((D::Path,WindowTag), WindowMsg<I>)>,window_path:D::Path) -> Self {
//         Self { sender,window_path, next_id: Arc::new(AtomicU64::new(1)), id: 0 }
//     }
// }

// impl<I: Send + 'static, D: Depth> Clone for WindowS<I,D> {
//     fn clone(&self) -> Self {
//         Self {
//             sender: self.sender.clone(),
//             window_path: self.window_path.clone(),
//             next_id: self.next_id.clone(),
//             id: self.next_id.fetch_add(1, Ordering::Relaxed),
//         }
//     }
// }

// impl<I,D> WindowAccumulator for WindowS<I,D>
// where
//     I: Data,
//     D: Depth + 'static
// {
//     type In = I;
//     type Out = ();

//     #[inline]
//     fn process(&mut self, el: &Self::In) {
//         let key = (self.window_path.clone(), self.id);
//         self.sender.send((key, WindowMsg::Item(el.clone()))).unwrap();
//     }

//     fn output(self) {
//         let key = (self.window_path.clone(), self.id);
//         self.sender.send((key,WindowMsg::WindowClosed)).unwrap();
//     }
// }

//versione per entrare nel contesto di StreamingWindow
impl<Key, Out, W, Op> WindowedStream<Op, Out, W>
where
    W: StreamingWindowDescription + WindowDescription<Out> + 'static,
    Op: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data + Debug,
{
    pub fn to_streaming_window(
        self,
    ) -> FirstClassWindowedStream<impl Operator<Out = (Key, WindowTag, WindowMsg<Out>)>, Level<Zero>> {
        let stream = self.inner;
        let tgr = self.descr.build_tagger();

        let manager = KeyedWindowTagger::<_, _, _>::new(tgr);

        // stream
        //     .add_operator(|prev| WindowOperator::new(prev, name.into(), manager))

        let new_s = stream
            .0
            .add_operator(|prev| WindowRouter::new(prev, std::any::type_name::<W>(), manager));

        //Crea canale per poter inviare gli elementi al nuovo blocco

        FirstClassWindowedStream::new(new_s)
    }
}

// //Versione per aumentare la profondità di un livello
// //ha ancora bisogno di essere completata

// impl<Op, WinOut, WinDescr, D,I> FirstClassWindowedWithDescr<Op,WinOut, WinDescr,Level<D>>
// where
//     WinDescr: WindowDescription<I> + 'static,
//     D:Depth + Debug,
//     Op: Operator<Out = (<Level<D> as Depth>::Path, WindowMsg<I>)> + 'static,
//     WinOut: 'static,
//     I: Data + Debug,
// {
//     pub fn to_streaming_window(self) -> FirstClassWindowedStream<impl Operator<Out = (<Level<D> as Depth>::Path, WindowMsg<I>)>, Level<Level<D>>> {
//         //Crea canale per poter inviare gli elementi al nuovo blocco
//         let (tx, source) =
//             ChannelSource::<((<Level<D> as Depth>::Path,u64), WindowMsg<I>)>::new(CHANNEL_SIZE, Replication::One);
//         let ctx = self.inner.0.ctx.clone();

//         //Chiudi blocco corrente con for_each che scarta l'output vuoto del nostro operatore (Key,())
//         self.add_window_operator("to_streaming_window", WindowS::new(tx,<Level<D> as Depth>::Path))
//             .for_each(|_| {});

//         let block = ctx
//             .lock()
//             .new_block(source, Default::default(), Default::default());
//         let downstream = Stream::new(ctx, block);

//         FirstClassWindowedStream::new(downstream)
//     }
// }

/// This operator abstracts the window logic as an operator and delegates to the
/// `KeyedWindowManager` and a `ProcessFunc` the job of building and processing the windows,
/// respectively.
#[derive(Clone)]
pub(crate) struct WindowRouter<K, I, Op, W>
where
    W: WindowTagger,
{
    /// The previous operators in the chain.
    prev: Op,
    /// The name of the actual operator that this one abstracts.
    ///
    /// It is used only for tracing purposes.
    name: String,
    /// The manager that will build the windows.
    tagger: KeyedWindowTagger<K, I, W>,
    /// A buffer for storing ready items.
    output_buffer: VecDeque<StreamElement<(K, WindowTag, WindowMsg<I>)>>,
}

impl<K, I, Prev, W> WindowRouter<K, I, Prev, W>
where
    W: WindowTagger,
{
    fn new(prev: Prev, name: &str, tagger: KeyedWindowTagger<K, I, W>) -> Self {
        Self {
            prev,
            name: name.into(),
            tagger,
            output_buffer: Default::default(),
        }
    }
}

impl<K, I, Prev, W> Display for WindowRouter<K, I, Prev, W>
where
    W: WindowTagger,
    Prev: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {} -> WindowRouter[{}]<{}>",
            self.prev,
            std::any::type_name::<W>(),
            self.name,
            std::any::type_name::<WindowMsg<I>>(),
        )
    }
}

impl<K, I, Prev, W> Operator for WindowRouter<K, I, Prev, W>
where
    W: WindowTagger + Send,
    Prev: Operator<Out = (K, I)>,
    K: DataKey,
    I: Data,
{
    type Out = (K, WindowTag, WindowMsg<I>);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> crate::operator::StreamElement<Self::Out> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                return item;
            }

            let el = self.prev.next();
            match el {
                el @ (StreamElement::Item(_) | StreamElement::Timestamped(_, _)) => {
                    let (key, el) = el.take_key(); // TODO: Handle timestamps
                    let key = key.unwrap();

                    let tgr = self
                        .tagger
                        .windows
                        .entry(key.clone())
                        .or_insert_with(|| self.tagger.init.clone());

                    let ret = tgr.map(el);
                    self.output_buffer.extend(
                        ret.into_iter()
                            .map(|e| StreamElement::Item((key.clone(), e.tag, e.item))),
                    );
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                el => {
                    let (_, el) = el.take_key();

                    self.tagger.windows.retain(|key, mgr| {
                        let ret = mgr.map(el.clone());
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|e| StreamElement::Item((key.clone(), e.tag, e.item))),
                        );
                        !mgr.recycle()
                    });

                    // Forward system messages and watermarks
                    let msg = match el {
                        StreamElement::Watermark(w) => StreamElement::Watermark(w),
                        StreamElement::Terminate => StreamElement::Terminate,
                        StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
                        _ => unreachable!(),
                    };
                    self.output_buffer.push_back(msg);
                }
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<(K, WindowMsg<I>), _>(&self.name))
    }
}
