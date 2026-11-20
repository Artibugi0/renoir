use crate::operator::streaming_window::filter::Filter;
use crate::operator::streaming_window::map::Map;
use crate::operator::streaming_window::window_operators::fold::Fold;
use crate::operator::window::{StreamingWindowDescription, WindowDescription};
use std::hash::Hash;
use std::marker::PhantomData;
// use crate::operator::streaming_window::window_operators::fold::NestedFold;
use crate::operator::{Data, DataKey, StreamElement};
use crate::stream::Stream;
use crate::KeyedStream;

use crate::{operator::Operator, stream::KeyedItem};

pub type TagId = u64;

#[derive(Clone, Debug)]
pub enum WindowItem<K, T, I> {
    Item { key: K, tag: T, item: I },
    Close { key: K, tag: T },
}

impl<K, T, I> WindowItem<K, T, I> {
    pub fn new_item(key: K, tag: T, item: I) -> Self {
        Self::Item { key, tag, item }
    }

    pub fn new_close(key: K, tag: T) -> Self {
        Self::Close { key, tag }
    }

    pub fn key(&self) -> &K {
        match self {
            WindowItem::Item { key, .. } => key,
            WindowItem::Close { key, .. } => key,
        }
    }

    pub fn tag(&self) -> &T {
        match self {
            WindowItem::Item { tag, .. } => tag,
            WindowItem::Close { tag, .. } => tag,
        }
    }

    pub fn map<O>(self, f: impl FnOnce(I) -> O) -> WindowItem<K, T, O> {
        match self {
            WindowItem::Item { key, tag, item } => WindowItem::Item {
                key,
                tag,
                item: (f)(item),
            },
            WindowItem::Close { key, tag } => WindowItem::Close { key, tag },
        }
    }

    pub fn strip(self) -> InnerWindowItem<(), I> {
        match self {
            WindowItem::Item { item, .. } => InnerWindowItem::Item { tag: (), item },
            WindowItem::Close { .. } => InnerWindowItem::Close { tag: () },
        }
    }
}

impl<K, T, I> WindowItem<K, ChainTag<T>, I> {
    pub fn from_parts(key: K, parent_tag: T, inner: InnerWindowItem<TagId, I>) -> Self {
        match inner {
            InnerWindowItem::Item { tag, item } => Self::Item {
                key,
                tag: ChainTag {
                    parent: parent_tag,
                    value: tag,
                },
                item,
            },
            InnerWindowItem::Close { tag } => Self::Close {
                key,
                tag: ChainTag {
                    parent: parent_tag,
                    value: tag,
                },
            },
        }
    }
}

#[derive(Clone)]
pub enum InnerWindowItem<T, I> {
    Item { tag: T, item: I },
    Close { tag: T },
}

impl<T, I> InnerWindowItem<T, I> {
    pub fn new_item(tag: T, item: I) -> Self {
        Self::Item { tag, item }
    }

    pub fn new_close(tag: T) -> Self {
        Self::Close { tag }
    }

    pub fn tag(&self) -> &T {
        match self {
            InnerWindowItem::Item { tag, .. } => tag,
            InnerWindowItem::Close { tag } => tag,
        }
    }

    pub fn map<O>(self, f: impl FnOnce(I) -> O) -> InnerWindowItem<T, O> {
        match self {
            InnerWindowItem::Item { tag, item } => InnerWindowItem::Item {
                tag,
                item: (f)(item),
            },
            InnerWindowItem::Close { tag } => InnerWindowItem::Close { tag },
        }
    }
}

pub trait WindowTag: Eq + Hash + Clone + Send {
    type Parent;

    fn get_parent(&self) -> &Self::Parent;
    fn into_parent(self) -> Self::Parent;
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct ChainTag<P> {
    parent: P,
    value: TagId,
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct NoTag;

impl WindowTag for NoTag {
    type Parent = NoTag;

    fn get_parent(&self) -> &Self::Parent {
        &NoTag
    }

    fn into_parent(self) -> Self::Parent {
        NoTag
    }
}

impl<P: WindowTag> WindowTag for ChainTag<P> {
    type Parent = P;

    fn get_parent(&self) -> &Self::Parent {
        &self.parent
    }

    fn into_parent(self) -> Self::Parent {
        self.parent
    }
}

pub struct FirstClassWindowedStream<Op, T=ChainTag<NoTag>>(Stream<Op>, PhantomData<T>)
where
    Op: Operator;

impl<Op> FirstClassWindowedStream<Op>
where
    Op: Operator,
{
    pub(crate) fn new(stream: Stream<Op>) -> Self {
        FirstClassWindowedStream(stream, PhantomData)
    }
}

impl<Op, T1> FirstClassWindowedStream<Op, T1>
where
    Op: Operator,
{
    pub(crate) fn add_operator<T2, Op2, GetOp>(
        self,
        get_operator: GetOp,
    ) -> FirstClassWindowedStream<Op2, T2>
    where
        Op2: Operator,
        GetOp: FnOnce(Op) -> Op2,
    {
        FirstClassWindowedStream(self.0.add_operator(get_operator), PhantomData)
    }
}

//qui si implementano i metodi che fanno uscire lo stream dal contesto di FirstClass (Close + Riduttori)
impl<K, I, Op> FirstClassWindowedStream<Op>
where
    Op: Operator<Out = WindowItem<K, ChainTag<NoTag>, I>>,
    K: DataKey,
    I: Data,
{
    //andrà modificata in futuro
    //ancora non è chiaro al 100% come uscire dal contesto di StreamingWindow
    //
    // pub fn close(self) -> KeyedStream<Op> {
    //     KeyedStream(self.0)
    // }

    pub(crate) fn add_operator_and_close<O, Op2, GetOp>(
        self,
        get_operator: GetOp,
    ) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        Op2: Operator<Out = WindowItem<K, NoTag, O>> + 'static,
        GetOp: FnOnce(Op) -> Op2,
        O: Data,
    {
        let new_s = self.0.add_operator(get_operator).flat_map(|w| match w {
            WindowItem::Item { key, item, .. } => Some((key, item)),
            WindowItem::Close { .. } => {
                tracing::debug!("ignoring window close");
                None
            }
        });
        KeyedStream(new_s)
    }
}

impl<K, I, Op> FirstClassWindowedStream<Op, ChainTag<NoTag>>
where
    Op: Operator<Out = WindowItem<K, ChainTag<NoTag>, I>> + 'static,
    K: DataKey,
{
    pub fn fold<O, F>(self, init: O, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        I: Data,
        O: Data,
        F: Fn(&mut O, I) + Data + 'static,
    {
        self.add_operator_and_close(|prev| Fold::new(prev, init, f))
    }
}

//qui si implementano i metodi che chiudono una finestra ma ci mantengono nel contesto (risaliamo di un livello di profondità) (Riduttori)
impl<K, T, I, Op> FirstClassWindowedStream<Op, ChainTag<ChainTag<T>>>
where
    Op: Operator<Out = WindowItem<K, ChainTag<ChainTag<T>>, I>>,
    I: Data,
    K: DataKey,
    T: WindowTag,
{
    pub fn fold<O, F>(
        self,
        init: O,
        f: F,
    ) -> FirstClassWindowedStream<impl Operator<Out = WindowItem<K, ChainTag<T>, O>>>
    where
        O: Data,
        F: Fn(&mut O, I) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Fold::new(prev, init, f))
    }
}

//qui si implementano i metodi che non modificano la profondità della finestra (Modificatori)
impl<K, T, I, Op> FirstClassWindowedStream<Op, T>
where
    Op: Operator<Out = WindowItem<K, T, I>> + 'static,
    I: Data,
    T: Data,
    K: DataKey,
{
    //filter è un'operazione non di riduzione ma di modifica degli elementi della finestra, dunque rimaniamo nel contesto
    pub fn filter<F>(
        self,
        predicate: F,
    ) -> FirstClassWindowedStream<impl Operator<Out = WindowItem<K, T, I>>>
    where
        F: Fn(&I) -> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| Filter::new(prev, predicate))
    }

    pub fn map<O: Data, F>(
        self,
        f: F,
    ) -> FirstClassWindowedStream<impl Operator<Out = WindowItem<K, T, O>>>
    where
        F: Fn(I) -> O + Send + Clone + 'static,
    {
        self.add_operator(|prev| Map::new(prev, f))
    }

    pub fn inspect<F>(
        self,
        f: F,
    ) -> FirstClassWindowedStream<impl Operator<Out = WindowItem<K, T, I>>>
    where
        F: FnMut(&Op::Out) + Send + Clone + 'static,
    {
        FirstClassWindowedStream(self.0.inspect(f), PhantomData)
    }
}
//Nested Windowing


pub struct FirstClassWindowedWithDescr<Op, T, W>
where
    Op: Operator,
{
    pub(crate) inner: FirstClassWindowedStream<Op, T>,
    pub(crate) descr: W,
}

impl<K, T, I, Op> FirstClassWindowedStream<Op, T>
where
    Op: Operator<Out = WindowItem<K, T, I>>,
    I: Data,
{
    pub fn window<W: StreamingWindowDescription>(
        self,
        descr: W,
    ) -> FirstClassWindowedWithDescr<Op, T, W> {
        FirstClassWindowedWithDescr {
            inner: self,
            descr,
        }
    }
}
