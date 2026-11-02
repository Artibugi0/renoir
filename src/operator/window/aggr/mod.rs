mod fold;
// mod columnar;
pub(super) use fold::{Fold, FoldFirst};

mod collect_vec;
mod count;
mod join;
mod max;
mod min;
mod nth;
mod sum;
mod sum_if;
mod count_distinct;
pub (crate) mod to_streaming_window;

#[cfg(feature = "parquet")]
mod to_arrow;
