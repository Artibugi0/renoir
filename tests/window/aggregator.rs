use itertools::Itertools;

use renoir::operator::source::IteratorSource;
use renoir::operator::window::CountWindow;

use super::utils::TestHelper;

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
fn test_distinct_count_tumbling(){
    TestHelper::local_remote_env(|env|{
        let s = [0,1,1,2,3,4,5,6,7,8,9,9,9]; //count should be 10
        let source = IteratorSource::new(s.into_iter());
        let res = env
        .stream(source)
        .window_all(CountWindow::tumbling(13))
        .count_distinct()
        .drop_key()    
        .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(
                res,vec![10]
            );
        }
    });
} 
#[test]
fn test_distinct_count_sliding(){
        TestHelper::local_remote_env(|env|{
        let s = [0,1,1,2,3,4,5,6,7,8,9,9,9,0,10];
        let source = IteratorSource::new(s.into_iter());
        let res = env
        .stream(source)
        .window_all(CountWindow::sliding(3,1))
        .count_distinct()
        .drop_key()    
        .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(
                res,vec![2,2,3,3,3,3,3,3,3,2,1,2,3]
            );
        }
    });
}