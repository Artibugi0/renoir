use crate::{
    WindowedStream, operator::{
        Data, DataKey, Operator, streaming_window::{router::WindowRouter, streamingwindow::{
            FirstClassWindowedStream, Level, WindowMsg, WindowTag, Zero,
        }}, window::{KeyedWindowTagger, StreamingWindowDescription, WindowDescription},
    },
};
use std::fmt::Debug;

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
    ) -> FirstClassWindowedStream<impl Operator<Out = (Key, WindowTag, WindowMsg<Out>)>, Level<Zero>>
    {
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
