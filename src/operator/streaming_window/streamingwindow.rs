use std::collections::VecDeque;
use std::fmt::Display;
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::block::OperatorStructure;
use crate::operator::streaming_window::filter::Filter;
use crate::operator::streaming_window::map::Map;
use crate::operator::streaming_window::window_operators::fold::Fold;
// use crate::operator::streaming_window::window_operators::fold::NestedFold;
use crate::operator::window::{
    KeyedWindowManager, WindowAccumulator, WindowDescription, WindowManager,
};
use crate::operator::{Data, DataKey, StreamElement};
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

fn control_msg<T, U>(el: StreamElement<T>) -> Option<StreamElement<U>> {
    match el {
        StreamElement::Watermark(w) => Some(StreamElement::Watermark(w)),
        StreamElement::Terminate => Some(StreamElement::Terminate),
        StreamElement::FlushAndRestart => Some(StreamElement::FlushAndRestart),
        StreamElement::FlushBatch => Some(StreamElement::FlushBatch),
        StreamElement::Item(_) | StreamElement::Timestamped(_, _) => None,
    }
}
#[derive(Clone)]
pub struct FirstClassWindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
{
    /// The previous operators in the chain.
    prev: Prev,
    /// The name of the actual operator that this one abstracts.
    ///
    /// It is used only for tracing purposes.
    name: String,
    /// The manager that will build the windows.
    manager: KeyedWindowManager<Key, In, Out, W>,
    /// A buffer for storing ready items.
    output_buffer: VecDeque<StreamElement<(Key, Out)>>,
}

impl<Op, WinOut, WinDescr, Key, In, D> FirstClassWindowedWithDescr<Op, WinOut, WinDescr, Level<D>>
where
    WinDescr: WindowDescription<In>,
    Op: Operator<Out = (Key, WindowMsg<In>)> + 'static,
    Key: DataKey,
    In: Data,
    D: Depth,
{
    pub(crate) fn add_window_operator<A, NewOut>(
        self,
        name: &str,
        accumulator: A,
    ) -> KeyedStream<impl Operator<Out = (Key, NewOut)>>
    where
        NewOut: Data,
        A: WindowAccumulator<In = In, Out = NewOut>,
    {
        let init = self.descr.build_manager::<A>(accumulator);
        let manager = KeyedWindowManager::new(init);

        KeyedStream(
            self.inner
                .0
                .add_operator(|prev| FirstClassWindowOperator::new(prev, name.into(), manager)),
        )
    }
}

impl<Key, In, Out, Prev, W> Display for FirstClassWindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
    Prev: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {} -> WindowOperator[{}]<{}>",
            self.prev,
            std::any::type_name::<W>(),
            self.name,
            std::any::type_name::<Out>(),
        )
    }
}
impl<Key, In, Out, Prev, W> FirstClassWindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager<In = In, Out = Out> + Send,
    Key: DataKey,
    In: Data,
    Out: Data,
{
    fn process_item(&mut self, key: Key, el: StreamElement<In>) {
        let mgr = self
            .manager
            .windows
            .entry(key.clone())
            .or_insert_with(|| self.manager.init.clone());

        let ret = mgr.process(el);
        self.output_buffer.extend(
            ret.into_iter()
                .map(|e| StreamElement::from(e).add_key(key.clone())),
        );
    }
}

impl<Key, In, Out, Prev, W> Operator for FirstClassWindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager<In = In, Out = Out> + Send,
    Prev: Operator<Out = (Key, WindowMsg<In>)>,
    Key: DataKey,
    In: Data,
    Out: Data,
{
    type Out = (Key, Out);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, Out)> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                return item;
            }

            let el = self.prev.next();
            match el {
                StreamElement::Item((id, WindowMsg::Item(item))) => {
                    self.process_item(id, StreamElement::Item(item));
                }
                StreamElement::Timestamped((id, WindowMsg::Item(item)), ts) => {
                    self.process_item(id, StreamElement::Timestamped(item, ts));
                }
                StreamElement::Item((id, WindowMsg::WindowClosed))
                | StreamElement::Timestamped((id, WindowMsg::WindowClosed), _) => {
                    let mgr = self
                        .manager
                        .windows
                        .entry(id.clone())
                        .or_insert_with(|| self.manager.init.clone());
                    let ret = mgr.process(StreamElement::FlushAndRestart);
                    self.output_buffer.extend(
                        ret.into_iter()
                            .map(|e| StreamElement::from(e).add_key(id.clone())),
                    );
                    // la finestra esterna è chiusa: libero lo stato di questa chiave
                    self.manager.windows.remove(&id);
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,

                el => {
                    let Some(el) = control_msg::<_, In>(el) else {
                        continue;
                    };
                    self.manager.windows.retain(|key, mgr| {
                        let ret = mgr.process(el.clone());
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|e| StreamElement::from(e).add_key(key.clone())),
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
            .add_operator(OperatorStructure::new::<(Key, Out), _>(&self.name))
    }
}

impl<Key, In, Out, Prev, W> FirstClassWindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
{
    pub(crate) fn new(
        prev: Prev,
        name: String,
        manager: KeyedWindowManager<Key, In, Out, W>,
    ) -> Self {
        Self {
            prev,
            name,
            manager,
            output_buffer: Default::default(),
        }
    }
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
