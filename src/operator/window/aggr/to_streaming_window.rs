use crate::{
    operator::{
        streaming_window::{
            router::WindowRouter,
            streamingwindow::{
                ChainTag, FirstClassWindowedStream, FirstClassWindowedWithDescr, NoTag, WindowItem,
                WindowTag,
            },
        },
        window::{KeyedWindowTagger, StreamingWindowDescription, WindowDescription},
        Data, DataKey, Operator,
    },
    WindowedStream,
};
use std::fmt::Debug;

//versione per entrare nel contesto di StreamingWindow
impl<K, I, W, Op> WindowedStream<Op, I, W>
where
    W: StreamingWindowDescription + WindowDescription<I> + 'static,
    Op: Operator<Out = (K, I)> + 'static,
    K: DataKey,
    I: Data + Debug,
{
    pub fn to_streaming_window(
        self,
    ) -> FirstClassWindowedStream<impl Operator<Out = WindowItem<K, ChainTag<NoTag>, I>>> {
        let stream = self.inner;
        let tgr = self.descr.build_tagger();

        let manager = KeyedWindowTagger::new(tgr);
        let name = std::any::type_name::<W>();

        // stream
        //     .add_operator(|prev| WindowOperator::new(prev, name.into(), manager))

        let new_s = stream
            .0
            .add_operator(|prev| WindowRouter::new(prev, name, manager));

        //Crea canale per poter inviare gli elementi al nuovo blocco

        FirstClassWindowedStream::new(new_s)
    }
}

//Versione per aumentare la profondità di un livello
//ha ancora bisogno di essere completata

impl<K, T, I, W, Op> FirstClassWindowedWithDescr<Op, T, W>
where
    W: StreamingWindowDescription,
    Op: Operator<Out = WindowItem<K, T, I>>,
    K: DataKey,
    T: WindowTag,
    I: Data,
{
    pub fn to_streaming_window(
        self,
    ) -> FirstClassWindowedStream<impl Operator<Out = WindowItem<K, ChainTag<T>, I>>, ChainTag<T>> {
        let stream = self.inner;
        let tgr = self.descr.build_tagger();

        let manager = KeyedWindowTagger::new(tgr);
        let name = std::any::type_name::<W>();

        // stream
        //     .add_operator(|prev| WindowOperator::new(prev, name.into(), manager))

        stream.add_operator(|prev| {
            WindowRouter::<K, T, ChainTag<T>, I, Op, W::Tagger>::new(prev, name, manager)
        })
    }
}
