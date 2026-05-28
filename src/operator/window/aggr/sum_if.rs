use std::ops::AddAssign;

use crate::operator::{Data, DataKey, Operator};
use super::{super::*,Fold};
use crate::stream::{KeyedStream, WindowedStream};

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn sum_if<F : Fn(&Out) -> bool + Clone + Send + 'static, NewOut: Data + Default + AddAssign<Out>>(
        self,
        pred : F
    ) -> KeyedStream<impl Operator<Out = (Key, NewOut)>> {
        let acc = Fold::new(NewOut::default(), move |sum: &mut NewOut, x: &Out| {
            if pred(&x) {
                *sum += x.clone()
            }
        });
  
        self.add_window_operator("WindowSumIf", acc)
    }
}