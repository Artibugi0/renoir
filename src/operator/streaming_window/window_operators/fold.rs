use std::collections::HashMap;
use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::streaming_window::streamingwindow::{ChainTag, NoTag, WindowItem, WindowTag};
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

//Questa implementazione di fold mantiene una hashmap di accumulatori
//ognuno associato ad una particolare finestra
//il resto dell'implementazione è simile a quella usata per stream normali

//TODO: l'implementazione,adesso come adesso quando prende windowclosed e lo consuma
//poichè come output
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<K, T, I, O, F, Op>
where
    O: Send + Clone,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = WindowItem<K, T, I>>,
    T: WindowTag,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: O,
    //hashmap con chiave id della finestra
    accumulators: HashMap<(K, T), (O, Option<Timestamp>)>,
    #[derivative(Debug = "ignore")]
    ready: Vec<StreamElement<WindowItem<K, T::Parent, O>>>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<K, T, I, O, F, Op> Display for Fold<K, T, I, O, F, Op>
where
    O: Data,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator<Out = WindowItem<K, T, I>>,
    I: Data,
    K: DataKey,
    T: WindowTag,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Fold<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<K, T, I, O, F, Op> Fold<K, T, I, O, F, Op>
where
    O: Data,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator,
    Op: Operator<Out = WindowItem<K, T, I>>,
    T: WindowTag,
{
    pub(crate) fn new(prev: Op, init: O, fold: F) -> Self {
        Fold {
            prev,
            fold,
            init,
            accumulators: HashMap::new(),
            ready: Vec::new(),
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<K, T, I, O, F, Op> Operator for Fold<K, ChainTag<T>, I, O, F, Op>
where
    O: Data,
    F: Fn(&mut O, I) + Send + Clone,
    Op: Operator,
    Op: Operator<Out = WindowItem<K, ChainTag<T>, I>>,
    I: Data,
    T: WindowTag,
    K: DataKey,
{
    //da capire che chiave associamo all'output al di fuori del contesto first class
    type Out = WindowItem<K, T, O>;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        while !self.received_end {
            match self.prev.next() {
                //match si aspetta elementi della finestra, accumulatori creati/aggiornati in base all'id della finestra
                //associato all'elemento
                StreamElement::Item(WindowItem::Item { key, tag, item }) => {
                    let (acc, _) = self
                        .accumulators
                        .entry((key, tag))
                        .or_insert_with(|| (self.init.clone(), None));
                    (self.fold)(acc, item);
                }
                StreamElement::Timestamped(WindowItem::Item { key, tag, item }, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    let (acc, t) = self
                        .accumulators
                        .entry((key, tag))
                        .or_insert_with(|| (self.init.clone(), None));
                    *t = Some(t.unwrap_or(ts).max(ts));
                    (self.fold)(acc, item);
                }
                //se ricevo windowclosed so che teoricamente elementi della finestra sono esauriti
                //sputo fuori l'accumulatore completato e cancello l'entry dalla mappa
                //
                // TODO: Devo mandare anche il windowclosed??
                StreamElement::Item(WindowItem::Close { key, tag })
                | StreamElement::Timestamped(WindowItem::Close { key, tag }, _) => {
                    if let Some((acc, ts)) = self.accumulators.remove(&(key.clone(), tag.clone())) {
                        let elem = WindowItem::new_item(key, tag.into_parent(), acc);
                        return match ts {
                            Some(ts) => StreamElement::Timestamped(elem, ts),
                            None => StreamElement::Item(elem),
                        };
                    }
                }
                //logica invariata
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        if !self.accumulators.is_empty() {
            // take a reference to move into the closure, avoiding moving "self"
            // let timestamps = &mut self.timestamps;
            self.ready.extend(self.accumulators.drain().map(
                |((key, tag), (value, timestamp))| {
                    if let Some(ts) = timestamp {
                        StreamElement::Timestamped(WindowItem::new_item(key, tag.into_parent(), value), ts)
                    } else {
                        StreamElement::Item(WindowItem::new_item(key, tag.into_parent(), value))
                    }
                },
            ));
        }

        // consume the ready items
        if let Some(elem) = self.ready.pop() {
            return elem;
        }

        // If watermark were received, send one downstream
        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>(
                "Streaming_Window_Fold_Not_Nested",
            ))
    }
}

// impl<K, I, O, F, Op> Operator for Fold<K, ChainTag<NoTag>, I, O, F, Op>
// where
//     O: Data,
//     F: Fn(&mut O, I) + Send + Clone,
//     Op: Operator,
//     Op: Operator<Out = WindowItem<K, ChainTag<NoTag>,I>>,
//     I: Data,
//     K: DataKey,
// {
//     //da capire che chiave associamo all'output al di fuori del contesto first class
//     type Out = (K, O);

//     fn setup(&mut self, metadata: &mut ExecutionMetadata) {
//         self.prev.setup(metadata);
//     }

//     fn next(&mut self) -> StreamElement<Self::Out> {
//         while !self.received_end {
//             match self.prev.next() {
//                 //match si aspetta elementi della finestra, accumulatori creati/aggiornati in base all'id della finestra
//                 //associato all'elemento
//                 StreamElement::Item(WindowItem::Item { key, tag, item }) => {
//                     let (acc, _) = self
//                         .accumulators
//                         .entry((key, tag))
//                         .or_insert_with(|| (self.init.clone(), None));
//                     (self.fold)(acc, item);
//                 }
//                 StreamElement::Timestamped(WindowItem::Item { key, tag, item }, ts) => {
//                     self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
//                     let (acc, t) = self
//                         .accumulators
//                         .entry((key, tag))
//                         .or_insert_with(|| (self.init.clone(), None));
//                     *t = Some(t.unwrap_or(ts).max(ts));
//                     (self.fold)(acc, item);
//                 }
//                 //se ricevo windowclosed so che teoricamente elementi della finestra sono esauriti
//                 //sputo fuori l'accumulatore completato e cancello l'entry dalla mappa
//                 StreamElement::Item(WindowItem::Close { key, tag})
//                 | StreamElement::Timestamped(WindowItem::Close { key, tag}, _) => {
//                     if let Some((acc, ts)) = self.accumulators.remove(&(key.clone(), tag)) {
//                         let elem = (key, acc);
//                         return match ts {
//                             Some(ts) => StreamElement::Timestamped(elem, ts),
//                             None => StreamElement::Item(elem),
//                         };
//                     }
//                 }
//                 //logica invariata
//                 StreamElement::Terminate => self.received_end = true,
//                 StreamElement::FlushAndRestart => {
//                     self.received_end = true;
//                     self.received_end_iter = true;
//                 }
//                 StreamElement::Watermark(ts) => {
//                     self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
//                 }
//                 // this block wont sent anything until the stream ends
//                 StreamElement::FlushBatch => {}
//             }
//         }

//         if !self.accumulators.is_empty() {
//             // take a reference to move into the closure, avoiding moving "self"
//             // let timestamps = &mut self.timestamps;
//             self.ready.extend(self.accumulators.drain().map(
//                 |((key, _tag), (value, timestamp))| {
//                     if let Some(ts) = timestamp {
//                         StreamElement::Timestamped((key, value), ts)
//                     } else {
//                         StreamElement::Item((key, value))
//                     }
//                 },
//             ));
//         }

//         // consume the ready items
//         if let Some(elem) = self.ready.pop() {
//             return elem;
//         }

//         // If watermark were received, send one downstream
//         if let Some(ts) = self.max_watermark.take() {
//             return StreamElement::Watermark(ts);
//         }

//         // the end was not really the end... just the end of one iteration!
//         if self.received_end_iter {
//             self.received_end_iter = false;
//             self.received_end = false;
//             return StreamElement::FlushAndRestart;
//         }

//         StreamElement::Terminate
//     }

//     fn structure(&self) -> BlockStructure {
//         self.prev
//             .structure()
//             .add_operator(OperatorStructure::new::<O, _>(
//                 "Streaming_Window_Fold_Not_Nested",
//             ))
//     }
// }

// #[derive(Clone, Derivative)]
// #[derivative(Debug)]
// pub(crate) struct NestedFold<O, F, Op, P, I>
// where
//     O: Send + Clone,
//     F: Fn(&mut O, I) + Send + Clone,
//     Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
//     P: DataKey,
// {
//     prev: Op,
//     #[derivative(Debug = "ignore")]
//     fold: F,
//     init: O,
//     //hashmap con chiave id della finestra
//     accumulators: HashMap<(P, u64), (O, Option<Timestamp>)>,
//     timestamp: Option<Timestamp>,
//     max_watermark: Option<Timestamp>,
//     received_end: bool,
//     received_end_iter: bool,
// }

// impl<O, F, Op, P, I> Display for NestedFold<O, F, Op, P, I>
// where
//     O: Data,
//     F: Fn(&mut O, I) + Send + Clone,
//     Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
//     P: DataKey,
//     I: Data,
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "{} -> NestedFold<{} -> {}>",
//             self.prev,
//             std::any::type_name::<I>(),
//             std::any::type_name::<O>()
//         )
//     }
// }

// impl<O, F, Op, P, I> NestedFold<O, F, Op, P, I>
// where
//     O: Data,
//     F: Fn(&mut O, I) + Send + Clone,
//     Op: Operator,
//     Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
//     P: DataKey,
// {
//     pub(crate) fn new(prev: Op, init: O, fold: F) -> Self {
//         NestedFold {
//             prev,
//             fold,
//             init,
//             accumulators: HashMap::new(),
//             timestamp: None,
//             max_watermark: None,
//             received_end: false,
//             received_end_iter: false,
//         }
//     }
// }

// impl<O, F, Op, P, I> Operator for NestedFold<O, F, Op, P, I>
// where
//     O: Data,
//     F: Fn(&mut O, I) + Send + Clone,
//     Op: Operator,
//     Op: Operator<Out = ((P, u64), WindowMsg<I>)>,
//     P: DataKey,
//     I: Data,
//     K: DataKey,
// {
//     type Out = (P, WindowMsg<O>);

//     fn setup(&mut self, metadata: &mut ExecutionMetadata) {
//         self.prev.setup(metadata);
//     }

//     fn next(&mut self) -> StreamElement<Self::Out> {
//         while !self.received_end {
//             match self.prev.next() {
//                 //match si aspetta elementi della finestra, accumulatori creati/aggiornati in base all'id della finestra
//                 //associato all'elemento
//                 StreamElement::Item(WindowItem::Item { key, tag, item }) => {
//                     let (acc, _) = self
//                         .accumulators
//                         .entry((key, id))
//                         .or_insert_with(|| (self.init.clone(), None));
//                     (self.fold)(acc, x);
//                 }
//                 //timestamp gestiti come nell'implementazione di base
//                 StreamElement::Timestamped(WindowItem::Item { key, tag, item }, ts) => {
//                     self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
//                     let (acc, t) = self
//                         .accumulators
//                         .entry((key, id))
//                         .or_insert_with(|| (self.init.clone(), None));
//                     *t = Some(t.unwrap_or(ts).max(ts));
//                     (self.fold)(acc, x);
//                 }
//                 //se ricevo windowclosed so che teoricamente elementi della finestra sono esauriti
//                 //sputo fuori l'accumulatore completato e cancello l'entry dalla mappa
//                 StreamElement::Item((id, WindowMsg::WindowClosed))
//                 | StreamElement::Timestamped((id, WindowMsg::WindowClosed), _) => {
//                     if let Some((acc, ts)) = self.accumulators.remove(&id) {
//                         let elem = (id.0, WindowMsg::Item(acc));
//                         return match ts {
//                             Some(ts) => StreamElement::Timestamped(elem, ts),
//                             None => StreamElement::Item(elem),
//                         };
//                     }
//                 }
//                 //logica invariata
//                 StreamElement::Terminate => self.received_end = true,
//                 StreamElement::FlushAndRestart => {
//                     self.received_end = true;
//                     self.received_end_iter = true;
//                 }
//                 StreamElement::Watermark(ts) => {
//                     self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
//                 }
//                 // this block wont sent anything until the stream ends
//                 StreamElement::FlushBatch => {}
//             }
//         }

//         // If watermark were received, send one downstream
//         if let Some(ts) = self.max_watermark.take() {
//             return StreamElement::Watermark(ts);
//         }

//         // the end was not really the end... just the end of one iteration!
//         if self.received_end_iter {
//             self.received_end_iter = false;
//             self.received_end = false;
//             return StreamElement::FlushAndRestart;
//         }

//         StreamElement::Terminate
//     }

//     fn structure(&self) -> BlockStructure {
//         self.prev
//             .structure()
//             .add_operator(OperatorStructure::new::<O, _>(
//                 "Streaming_Window_NestedFold",
//             ))
//     }
// }
