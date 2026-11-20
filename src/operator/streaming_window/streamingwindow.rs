use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::operator::streaming_window::filter::Filter;
use crate::operator::streaming_window::map::Map;
use crate::operator::streaming_window::window_operators::fold::Fold;
// use crate::operator::streaming_window::window_operators::fold::NestedFold;
use crate::operator::window::{WindowDescription};
use crate::operator::{Data, DataKey};
use crate::stream::Stream;
use crate::KeyedStream;

use crate::{operator::Operator, stream::KeyedItem};

pub type WindowTag = u64;

pub type KWElement<K, I> = (K, WindowTag, WindowMsg<I>);

// proposta di enum per classificare tipi di dati provenienti dalla finestra
//Item wrappa ogni elemento ricevuto dall'accumulatore prima di inviarlo al nuovo blocco
//WindowClosed indica la fine degli elementi ricevuti dall'operatore
//da capire come gestire finestre a più livelli
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowElement<T, I> {
    // pub(crate) key: K,
    pub(crate) tag: T,
    pub(crate) item: WindowMsg<I>,
}

impl<T, I> WindowElement<T, I> {
    pub fn new_item(tag: T, item: I) -> Self {
        Self {
            tag,
            item: WindowMsg::Item(item),
        }
    }

    pub fn new_close(tag: T) -> Self {
        Self {
            tag,
            item: WindowMsg::WindowClosed,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WindowMsg<I> {
    Item(I),
    WindowClosed,
}
//proposta di Trait per rinforzare la relazione fra lunghezza di chiave e profondità windowed stream
//evitare lunghezze di chiavi diverse dalla profondità della finestra
pub trait Depth {
    //tipo che conserva la struttura che la chiave per tutte le profondità
    //E' lista concatenata ((() , id1) , id2) (finestra a due livelli)
    type Path: DataKey;
    //valore della profondità
    const N: usize;
}

pub struct Zero;
impl Depth for Zero {
    type Path = ();
    const N: usize = 0;
}
pub struct Level<I>(PhantomData<I>);

impl<D: Depth> Depth for Level<D> {
    type Path = (D::Path, u64);
    const N: usize = D::N + 1;
}

pub struct FirstClassWindowedStream<Op, D = Level<Zero>>(pub Stream<Op>, PhantomData<D>)
where
    Op: Operator,
    D: Depth;

impl<Op, D> FirstClassWindowedStream<Op, D>
where
    Op: Operator,
    D: Depth,
{
    pub(crate) fn new(stream: Stream<Op>) -> Self {
        FirstClassWindowedStream(stream, PhantomData)
    }
}

impl<K, I, Op, D> FirstClassWindowedStream<Op, D>
where
    Op: Operator<Out = KWElement<K, I>> + 'static,
    D: Depth,
{
    pub(crate) fn add_operator<O, Op2, GetOp>(
        self,
        get_operator: GetOp,
    ) -> FirstClassWindowedStream<Op2>
    where
        Op2: Operator<Out = KWElement<K, O>>,
        GetOp: FnOnce(Op) -> Op2,
    {
        FirstClassWindowedStream(self.0.add_operator(get_operator), PhantomData)
    }
}

//qui si implementano i metodi che fanno uscire lo stream dal contesto di FirstClass (Close + Riduttori)
impl<K, I, Op> FirstClassWindowedStream<Op, Level<Zero>>
where
    Op: Operator<Out = KWElement<K, I>>,
{
    //andrà modificata in futuro
    //ancora non è chiaro al 100% come uscire dal contesto di StreamingWindow
    //
    // pub fn close(self) -> KeyedStream<Op> {
    //     KeyedStream(self.0)
    // }

    pub(crate) fn add_operator_and_close<Op2, GetOp>(self, get_operator: GetOp) -> KeyedStream<Op2>
    where
        Op2: Operator,
        Op2::Out: KeyedItem,
        GetOp: FnOnce(Op) -> Op2,
    {
        KeyedStream(self.0.add_operator(get_operator))
    }
}

impl<K, I, Op> FirstClassWindowedStream<Op, Level<Zero>>
where
    Op: Operator + 'static,
    Op: Operator<Out = (K, WindowTag, WindowMsg<I>)>,
    K: DataKey,
{
    pub fn fold<O, F>(self, init: O, f: F) -> KeyedStream<Fold<K, I, O, F, Op>>
    where
        O: Data,
        F: Fn(&mut O, I) + Data + 'static,
        I: Data,
    {
        self.add_operator_and_close(|prev| Fold::new(prev, init, f))
    }
}

// //qui si implementano i metodi che chiudono una finestra ma ci mantengono nel contesto (risaliamo di un livello di profondità) (Riduttori)
// impl<Op, D, I> FirstClassWindowedStream<Op, Level<Level<D>>>
// where
//     D: Depth,
//     Op: Operator<Out = (<Level<Level<D>> as Depth>::Path, WindowMsg<I>)> + 'static,
//     I: Data,
// {
//     pub fn fold<O, F>(
//         self,
//         init: O,
//         f: F,
//     ) -> FirstClassWindowedStream<
//         impl Operator<Out = (<Level<D> as Depth>::Path, WindowMsg<O>)>,
//         Level<D>,
//     >
//     where
//         O: Data,
//         F: Fn(&mut O, I) + Send + Clone + 'static,
//     {
//         self.add_operator(|prev| NestedFold::new(prev, init, f))
//     }
// }

//qui si implementano i metodi che non modificano la profondità della finestra (Modificatori)
impl<K, I, Op> FirstClassWindowedStream<Op>
where
    Op: Operator<Out = KWElement<K, I>> + 'static,
    I: Data,
    K: DataKey,
{
    //filter è un'operazione non di riduzione ma di modifica degli elementi della finestra, dunque rimaniamo nel contesto
    pub fn filter<F>(
        self,
        predicate: F,
    ) -> FirstClassWindowedStream<impl Operator<Out = (K, WindowTag, WindowMsg<I>)>>
    where
        F: Fn(&I) -> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| Filter::new(prev, predicate))
    }

    pub fn map<O: Data, F>(
        self,
        f: F,
    ) -> FirstClassWindowedStream<impl Operator<Out = (K, WindowTag, WindowMsg<O>)>>
    where
        F: Fn(I) -> O + Send + Clone + 'static,
    {
        self.add_operator(|prev| Map::new(prev, f))
    }

    pub fn inspect<F>(
        self,
        f: F,
    ) -> FirstClassWindowedStream<impl Operator<Out = (K, WindowTag, WindowMsg<I>)>>
    where
        F: FnMut(&Op::Out) + Send + Clone + 'static,
    {
        FirstClassWindowedStream(self.0.inspect(f), PhantomData)
    }
}

//Nested Windowing

pub struct FirstClassWindowedWithDescr<Op, WinOut, WinDescr, D>
where
    Op: Operator,
    Op::Out: KeyedItem,
    D: Depth,
{
    pub(crate) inner: FirstClassWindowedStream<Op, D>,
    pub(crate) descr: WinDescr,
    pub(crate) _win_out: PhantomData<WinOut>,
}

impl<Op, D, I> FirstClassWindowedStream<Op, Level<D>>
where
    D: Depth,
    Op: Operator<Out = (<Level<D> as Depth>::Path, WindowMsg<I>)> + 'static,
    I: Data,
{
    pub fn window<WinOut: Data, WinDescr: WindowDescription<I>>(
        self,
        descr: WinDescr,
    ) -> FirstClassWindowedWithDescr<Op, WinOut, WinDescr, Level<D>> {
        FirstClassWindowedWithDescr {
            inner: self,
            descr,
            _win_out: PhantomData,
        }
    }
}

