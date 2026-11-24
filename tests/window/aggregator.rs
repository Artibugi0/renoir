use itertools::Itertools;

use renoir::operator::source::{IteratorSource, ParallelIteratorSource};
use renoir::operator::window::{CountWindow, EventTimeWindow};

use std::sync::{Arc, Mutex};

use renoir::block::Replication;

use super::utils::{TestHelper, TimestampRecorder};

#[test]
fn test_first_window() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .window_all(CountWindow::sliding(3, 2))
            .first()
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    0, // [0, 1, 2]
                    2, // [2, 3, 4]
                    4, // [4, 5, 6]
                    6, // [6, 7, 8]
                       // 8, // [8, 9]
                ]
            );
        }
    });
}

#[test]
fn test_fold_window() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .window_all(CountWindow::sliding(3, 2))
            .fold(0, |acc, x| *acc += x)
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    3,  // [0, 1, 2]
                    9,  // [2, 3, 4]
                    15, // [4, 5, 6]
                    21, // [6, 7, 8]
                        // 17, // [8, 9]
                ]
                .into_iter()
                .sorted()
                .collect_vec()
            );
        }
    });
}

#[test]
fn test_sum_window() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .window_all(CountWindow::sliding(3, 2))
            .sum::<u8>()
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    3,  // [0, 1, 2]
                    9,  // [2, 3, 4]
                    15, // [4, 5, 6]
                    21, // [6, 7, 8]
                        // 17, // [8, 9]
                ]
                .into_iter()
                .sorted()
                .collect_vec()
            );
        }
    });
}

#[test]
fn test_min_window() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .window_all(CountWindow::sliding(3, 2))
            .min()
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    0, // [0, 1, 2]
                    2, // [2, 3, 4]
                    4, // [4, 5, 6]
                    6, // [6, 7, 8]
                       // 8, // [8, 9]
                ]
            );
        }
    });
}

#[test]
fn test_max_window() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .window_all(CountWindow::sliding(3, 2))
            .max()
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    2, // [0, 1, 2]
                    4, // [2, 3, 4]
                    6, // [4, 5, 6]
                    8, // [6, 7, 8]
                       // 9, // [8, 9]
                ]
            );
        }
    });
}

#[test]
fn test_map_window() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..10u16);
        let res = env
            .stream(source)
            .window_all(CountWindow::sliding(3, 2))
            .map(|items| {
                let mut res = 1;
                for x in items {
                    res *= x;
                }
                res
            })
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    0,         // [0, 1, 2]
                    2 * 3 * 4, // [2, 3, 4]
                    4 * 5 * 6, // [4, 5, 6]
                    6 * 7 * 8, // [6, 7, 8]
                               // 8 * 9,     // [8, 9]
                ]
                .into_iter()
                .sorted()
                .collect_vec()
            );
        }
    });
}

#[test]
fn test_distinct_count_tumbling() {
    TestHelper::local_remote_env(|env| {
        let s = [0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9]; //count should be 10
        let source = IteratorSource::new(s.into_iter());
        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(13))
            .count_distinct()
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, vec![10]);
        }
    });
}
#[test]
fn test_distinct_count_sliding() {
    TestHelper::local_remote_env(|env| {
        let s = [0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 0, 10];
        let source = IteratorSource::new(s.into_iter());
        let res = env
            .stream(source)
            .window_all(CountWindow::sliding(3, 1))
            .count_distinct()
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, vec![2, 2, 3, 3, 3, 3, 3, 3, 3, 2, 1, 2, 3]);
        }
    });
}

#[test]
fn test_sum_if() {
    TestHelper::local_remote_env(|env| {
        let s = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let source = IteratorSource::new(s.into_iter());
        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(10))
            .sum_if::<_, i32>(|x| *x > 5)
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, vec![30])
        }
    });
}

#[test]
fn streaming_window_test() {
    TestHelper::local_remote_env(|env| {
        let s = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let source = IteratorSource::new(s.into_iter());
        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(5))
            .to_streaming_window()
            .filter(|n| n % 2 == 0)
            .fold(0, |acc, x| *acc += x)
            .drop_key()
            .collect_vec();

        env.execute_blocking();

        if let Some(mut res) = res.get() {
            res.sort();
            assert_eq!(res, vec![6, 14]);
        }
    });
}

#[test]
fn streaming_window_group_test() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..100);
        let res = env
            .stream(source)
            .group_by(|x| x / 10)
            .window(CountWindow::new(3, 3, false))
            .to_streaming_window()
            .filter(|n| n % 2 == 1)
            .fold(0, |acc, x| *acc += x)
            .collect_vec();

        let h = env.config().host_id().unwrap();
        env.execute_blocking();

        let mut reference = Vec::new();

        for i in 0..10 {
            let mut wins = [0i32; 4];
            for j in 0..10 {
                if j % 2 == 1 {
                    let n = j + i * 10;
                    wins[j as usize / 3] += n;
                }
            }
            reference.extend(wins.into_iter().filter(|&x| x != 0).map(|x| (i, x)));
        }

        if h == 0 {
            let mut res = res.get().unwrap();
            res.sort();
            reference.sort();
            assert_eq!(reference, res,);
        }
    });
}

#[test]
fn streaming_window_map_test() {
    TestHelper::local_remote_env(|env| {
        let s = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let source = IteratorSource::new(s.into_iter());
        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(5))
            .to_streaming_window()
            .map(|x| x * 2)
            .fold(0, |acc, x| *acc += x)
            .drop_key()
            .collect_vec();
        env.execute_blocking();

        if let Some(mut res) = res.get() {
            res.sort();
            assert_eq!(res, vec![30, 80]);
        }
    });
}

#[test]
fn streaming_window_test_multiple() {
    TestHelper::local_remote_env(|env| {
        let s = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let source = IteratorSource::new(s.into_iter());
        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(4))
            .to_streaming_window()
            .filter(|n| n % 2 == 0)
            .window(CountWindow::tumbling(2))
            .to_streaming_window()
            .fold(0, |acc, x| *acc += x)
            .fold(0, |acc, x| *acc += x)
            .drop_key()
            .collect_vec();

        env.execute_blocking();

        if let Some(mut res) = res.get() {
            res.sort();
            assert_eq!(res, vec![2, 10, 18]);
        }
    });
}

#[test]
fn streaming_window_test_nested() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..30);
        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(10))
            .to_streaming_window()
            .window(CountWindow::tumbling(2))
            .to_streaming_window()
            .fold(0, |acc, x| *acc += x)
            .fold(1, |acc, x| *acc *= x)
            .drop_key()
            .collect_vec();

        env.execute_blocking();

        if let Some(mut res) = res.get() {
            res.sort();
            assert_eq!(
                res,
                vec![
                    (0 + 1) * (2 + 3) * (4 + 5) * (6 + 7) * (8 + 9),
                    (10 + 11) * (12 + 13) * (14 + 15) * (16 + 17) * (18 + 19),
                    (20 + 21) * (22 + 23) * (24 + 25) * (26 + 27) * (28 + 29),
                ]
            );
        }
    });
}

/// The outer fold must emit each outer window when that window closes, not all at the end of the
/// stream. Results are checked in order: flushing at the end would emit them in arbitrary order.
#[test]
fn streaming_window_test_nested_emits_on_close() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..300u64);
        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(10))
            .to_streaming_window()
            .window(CountWindow::tumbling(2))
            .to_streaming_window()
            .fold(0, |acc, x| *acc += x)
            .fold(1, |acc, x| *acc *= x)
            .drop_key()
            .collect_vec();

        env.execute_blocking();

        if let Some(res) = res.get() {
            let expected = (0..30u64)
                .map(|w| (0..5).map(|p| (10 * w + 2 * p) * 2 + 1).product::<u64>())
                .collect::<Vec<_>>();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn streaming_window_test_nested_event_time() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new(0..40u64);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let res = env
            .stream(source)
            .add_timestamps(|&x| x as i64, |_, &ts| Some(ts))
            .window_all(EventTimeWindow::tumbling(10))
            .to_streaming_window()
            // the last inner window of each outer window ends after the outer one: [8, 12)
            .window(EventTimeWindow::tumbling(4))
            .to_streaming_window()
            .fold(0, |acc, x| *acc += x)
            .fold(1, |acc, x| *acc *= x)
            .drop_key()
            .replication(Replication::One)
            .add_operator(|prev| TimestampRecorder::new(prev, seen.clone()))
            .collect_vec();

        env.execute_blocking();

        if let Some(res) = res.get() {
            // outer windows of 10 items, split in inner windows of 4, 4 and 2 items
            let expected = (0..4u64)
                .map(|w| {
                    let s = w * 10;
                    (s..s + 4).sum::<u64>() * (s + 4..s + 8).sum::<u64>() * (s + 8..s + 10).sum::<u64>()
                })
                .collect::<Vec<_>>();
            assert_eq!(res, expected);

            // results are stamped with the end of their outer window, not of the last inner one
            let seen = seen.lock().unwrap();
            let results: Vec<_> = seen.iter().filter(|(w, _)| !w).map(|(_, ts)| *ts).collect();
            assert_eq!(results, vec![10, 20, 30, 40]);
            // watermarks are not held back until the end of the stream
            let first_watermark = seen.iter().position(|(w, _)| *w).unwrap();
            let last_result = seen.iter().rposition(|(w, _)| !w).unwrap();
            assert!(first_watermark < last_result);
        }
    });
}

/// Outer event time windows of 10 items, split in count windows of 3 items. The last count window
/// of each outer window is incomplete (1 item) and is flushed when the outer window closes.
#[test]
fn streaming_window_test_event_time_then_count() {
    TestHelper::local_remote_env(|env| {
        let res = env
            .stream(IteratorSource::new(0..40u64))
            .add_timestamps(|&x| x as i64, |_, &ts| Some(ts))
            .window_all(EventTimeWindow::tumbling(10))
            .to_streaming_window()
            .window(CountWindow::new(3, 3, false))
            .to_streaming_window()
            .fold(0, |acc, x| *acc += x)
            .fold(1, |acc, x| *acc *= x)
            .drop_key()
            .collect_vec();

        env.execute_blocking();

        if let Some(res) = res.get() {
            let expected = (0..4u64)
                .map(|w| {
                    let s = w * 10;
                    (s..s + 3).sum::<u64>()
                        * (s + 3..s + 6).sum::<u64>()
                        * (s + 6..s + 9).sum::<u64>()
                        * (s + 9..s + 10).sum::<u64>()
                })
                .collect::<Vec<_>>();
            assert_eq!(res, expected);
        }
    });
}

/// Same as above with exact count windows: the incomplete window must be dropped.
#[test]
#[ignore = "Fold keeps the accumulators of dropped windows and emits them at the end of the stream"]
fn streaming_window_test_event_time_then_exact_count() {
    TestHelper::local_remote_env(|env| {
        let res = env
            .stream(IteratorSource::new(0..40u64))
            .add_timestamps(|&x| x as i64, |_, &ts| Some(ts))
            .window_all(EventTimeWindow::tumbling(10))
            .to_streaming_window()
            .window(CountWindow::tumbling(3))
            .to_streaming_window()
            .fold(0, |acc, x| *acc += x)
            .fold(1, |acc, x| *acc *= x)
            .drop_key()
            .collect_vec();

        env.execute_blocking();

        if let Some(res) = res.get() {
            let expected = (0..4u64)
                .map(|w| {
                    let s = w * 10;
                    (s..s + 3).sum::<u64>() * (s + 3..s + 6).sum::<u64>() * (s + 6..s + 9).sum::<u64>()
                })
                .collect::<Vec<_>>();
            assert_eq!(res, expected);
        }
    });
}

/// Outer count windows of 10 items, split in event time windows of 4 timestamps.
#[test]
fn streaming_window_test_count_then_event_time() {
    TestHelper::local_remote_env(|env| {
        let res = env
            .stream(IteratorSource::new(0..40u64))
            .add_timestamps(|&x| x as i64, |_, &ts| Some(ts))
            .window_all(CountWindow::tumbling(10))
            .to_streaming_window()
            .window(EventTimeWindow::tumbling(4))
            .to_streaming_window()
            .fold(0, |acc, x| *acc += x)
            .fold(1, |acc, x| *acc *= x)
            .drop_key()
            .collect_vec();

        env.execute_blocking();

        if let Some(res) = res.get() {
            let expected = (0..4u64)
                .map(|w| {
                    let s = w * 10;
                    (s..s + 4).sum::<u64>()
                        * (s + 4..s + 8).sum::<u64>()
                        * (s + 8..s + 10).sum::<u64>()
                })
                .collect::<Vec<_>>();
            assert_eq!(res, expected);
        }
    });
}

/*
        //#[test]
fn streaming_window_key_per_window() {
    TestHelper::local_remote_env(|env| {
        let source = IteratorSource::new((0u32..10).into_iter());

        let res = env
            .stream(source)
            .window_all(CountWindow::tumbling(5))
            .to_streaming_window()
            .close()
            .collect_vec();             // (id_finestra, elem)

        env.execute_blocking();

        if let Some(res) = res.get() {
            let mut groups: std::collections::BTreeMap<u64, Vec<u32>> = Default::default();
            for (id, v) in res {
                groups.entry(id).or_default().push(v);
            }
            let mut contents: Vec<Vec<u32>> = groups
                .into_values()
                .map(|mut v| { v.sort(); v })
                .collect();
            contents.sort();

            // due finestre, ognuna con la propria chiave
            assert_eq!(contents, vec![vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9]]);
        }
    });
}*/
