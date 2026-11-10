use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::marker::PhantomData;

use crate::{KeyedStream, WindowedStream};
use crate::block::OperatorStructure;
use crate::operator::{Data, DataKey, StreamElement};
use crate::operator::streaming_window::filter::Filter;
use crate::operator::streaming_window::fold::Fold;
use crate::operator::window::{KeyedWindowManager, WindowAccumulator, WindowDescription, WindowManager};
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
impl<I> WindowMsg<I> {
    /// Riferimento al dato, se è un Item.
    #[inline]
    pub(crate) fn item(&self) -> Option<&I> {
        match self {
            WindowMsg::Item(x) => Some(x),
            WindowMsg::WindowClosed => None,
        }
    }
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

pub struct FirstClassWindowedWithDescr<OperatorChain, WinOut, WinDescr>
where
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem,
{
    pub(crate) inner: FirstClassWindowedStream<OperatorChain>,
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
pub(crate) struct FirstClassWindowOperator<Key, In, Out, Prev, W>
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

impl<Op, WinOut, WinDescr,Key,In> FirstClassWindowedWithDescr<Op, WinOut, WinDescr>
where
    WinDescr: WindowDescription<In>,
    Op: Operator<Out = (Key, WindowMsg<In>)> + 'static,
    Key: DataKey,
    In: Data,
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
        let init = self.descr.build::<A>(accumulator);
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
    Prev: Operator<Out = (Key,WindowMsg<In>)>,
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
                StreamElement::Item((id,WindowMsg::Item(item))) => {
                   self.process_item(id, StreamElement::Item(item));
                }
                StreamElement::Timestamped((id,WindowMsg::Item(item)),ts ) => {
                    self.process_item(id, StreamElement::Timestamped(item,ts));
                }
                StreamElement::Item((id,WindowMsg::WindowClosed)) 
                | StreamElement::Timestamped((id,WindowMsg::WindowClosed ),_) => {
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
                    let Some(el) = control_msg::<_, In>(el) else { continue };
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

impl<Key, In, Op> FirstClassWindowedStream<Op>
where
    Op: Operator<Out = (Key, WindowMsg<In>)> + 'static,
    Key: DataKey,
    In: Data,
{
    pub fn window<WinOut: Data, WinDescr: WindowDescription<In>>(
        self,
        descr: WinDescr,
    ) -> FirstClassWindowedWithDescr<Op, WinOut, WinDescr> {
        FirstClassWindowedWithDescr { inner: self, descr, _win_out: PhantomData }
    }
}
