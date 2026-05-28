use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};
use std::collections::HashSet;
use std::hash::Hash;

#[derive(Clone)]
pub(crate) struct CountDistinct<T>(usize,HashSet<T>);

impl <T: Data + Ord + Hash> WindowAccumulator for CountDistinct<T>{
    type In = T;
    type Out = usize;

    #[inline]
    fn process(&mut self, el: &Self::In) {
        if !self.1.contains(el) {
            self.0 += 1;
            self.1.insert(el.clone());
        }
    }
    #[inline]
    fn output(self) -> Self::Out {
        self.0
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where 
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data + Ord + Hash,
{
    pub fn count_distinct(self) -> KeyedStream<impl Operator<Out = (Key, usize)>> {
        let acc= CountDistinct(0,HashSet::new());
        self.add_window_operator("WindowCountDistinct", acc)
    }
}


