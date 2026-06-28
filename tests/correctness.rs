use lfqueue::{BoundedQueue, MpmcQueue, MutexQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

// -----------------------------------------------------------------------
// Single-thread functional tests
// -----------------------------------------------------------------------

fn empty_and_full<Q: BoundedQueue<i32>>() {
    let queue = Q::with_capacity(4);

    assert_eq!(queue.capacity(), 4);
    assert!(queue.pop().is_none());

    assert!(queue.push(1).is_ok());
    assert!(queue.push(2).is_ok());
    assert!(queue.push(3).is_ok());
    assert!(queue.push(4).is_ok());
    assert!(queue.push(5).is_err());

    assert_eq!(queue.pop(), Some(1));
    assert_eq!(queue.pop(), Some(2));
    assert_eq!(queue.pop(), Some(3));
    assert_eq!(queue.pop(), Some(4));
    assert!(queue.pop().is_none());
}

fn wraparound_order<Q: BoundedQueue<i32>>() {
    let queue = Q::with_capacity(2);

    assert!(queue.push(10).is_ok());
    assert!(queue.push(20).is_ok());
    assert!(queue.push(30).is_err());

    assert_eq!(queue.pop(), Some(10));
    assert!(queue.push(30).is_ok());

    assert_eq!(queue.pop(), Some(20));
    assert_eq!(queue.pop(), Some(30));
    assert!(queue.pop().is_none());
}

// MutexQueue-specific: exercises an odd capacity (3) that MpmcQueue would
// reject at construction since it requires a power of two.
fn exact_capacity<Q: BoundedQueue<i32>>() {
    let queue = Q::with_capacity(3);

    assert_eq!(queue.capacity(), 3);
    assert!(queue.push(1).is_ok());
    assert!(queue.push(2).is_ok());
    assert!(queue.push(3).is_ok());
    assert!(queue.push(4).is_err());

    assert_eq!(queue.pop(), Some(1));
    assert!(queue.push(4).is_ok());

    assert_eq!(queue.pop(), Some(2));
    assert_eq!(queue.pop(), Some(3));
    assert_eq!(queue.pop(), Some(4));
    assert!(queue.pop().is_none());
}

fn move_only_values<Q: BoundedQueue<Box<i32>>>() {
    let queue = Q::with_capacity(2);

    assert!(queue.push(Box::new(42)).is_ok());
    let value = queue.pop();
    assert_eq!(value, Some(Box::new(42)));
    assert!(queue.pop().is_none());
}

#[test]
fn mpmc_empty_and_full() {
    empty_and_full::<MpmcQueue<i32>>();
}

#[test]
fn mutex_empty_and_full() {
    empty_and_full::<MutexQueue<i32>>();
}

#[test]
fn mpmc_wraparound_order() {
    wraparound_order::<MpmcQueue<i32>>();
}

#[test]
fn mutex_wraparound_order() {
    wraparound_order::<MutexQueue<i32>>();
}

#[test]
fn mutex_exact_capacity() {
    exact_capacity::<MutexQueue<i32>>();
}

#[test]
fn mpmc_move_only_values() {
    move_only_values::<MpmcQueue<Box<i32>>>();
}

#[test]
fn mutex_move_only_values() {
    move_only_values::<MutexQueue<Box<i32>>>();
}

#[test]
#[should_panic(expected = "non-zero power of two")]
fn mpmc_rejects_zero_capacity() {
    MpmcQueue::<i32>::new(0);
}

#[test]
#[should_panic(expected = "non-zero power of two")]
fn mpmc_rejects_non_power_of_two() {
    MpmcQueue::<i32>::new(3);
}

#[test]
#[should_panic(expected = "non-zero")]
fn mutex_rejects_zero_capacity() {
    MutexQueue::<i32>::new(0);
}

// -----------------------------------------------------------------------
// Concurrent stress tests
// -----------------------------------------------------------------------

/// `producers` threads each push a unique sequence of values; `consumers`
/// threads pop until all `producers * values_per_producer` items are
/// received. Verifies every value pushed was received exactly once (no
/// duplicates, no drops) — this catches the most common class of
/// lock-free bugs.
fn run_stress<Q>(capacity: usize, producers: usize, consumers: usize, values_per_producer: i32)
where
    Q: BoundedQueue<i32> + Send + Sync,
{
    let queue = Q::with_capacity(capacity);
    let total = producers as i32 * values_per_producer;

    let consumed = AtomicUsize::new(0);
    let received: Mutex<Vec<i32>> = Mutex::new(Vec::with_capacity(total as usize));

    thread::scope(|scope| {
        for p in 0..producers {
            let queue = &queue;
            scope.spawn(move || {
                let start = p as i32 * values_per_producer;
                let end = start + values_per_producer;
                for v in start..end {
                    let mut pending = v;
                    while let Err(returned) = queue.push(pending) {
                        pending = returned;
                    }
                }
            });
        }

        for _ in 0..consumers {
            let queue = &queue;
            let consumed = &consumed;
            let received = &received;
            scope.spawn(move || {
                let mut local = Vec::new();
                while consumed.load(Ordering::Relaxed) < total as usize {
                    if let Some(value) = queue.pop() {
                        local.push(value);
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                received.lock().unwrap().extend(local);
            });
        }
    });

    let mut received = received.into_inner().unwrap();
    assert_eq!(received.len(), total as usize);
    received.sort_unstable();
    for (i, &value) in received.iter().enumerate() {
        assert_eq!(value, i as i32);
    }
}

const VPROD: i32 = 10_000;

#[test]
fn mpmc_stress_1p_1c() {
    run_stress::<MpmcQueue<i32>>(8, 1, 1, VPROD);
}

#[test]
fn mutex_stress_1p_1c() {
    run_stress::<MutexQueue<i32>>(8, 1, 1, VPROD);
}

#[test]
fn mpmc_stress_2p_2c() {
    run_stress::<MpmcQueue<i32>>(8, 2, 2, VPROD);
}

#[test]
fn mutex_stress_2p_2c() {
    run_stress::<MutexQueue<i32>>(8, 2, 2, VPROD);
}

#[test]
fn mpmc_stress_4p_4c() {
    run_stress::<MpmcQueue<i32>>(64, 4, 4, VPROD);
}

#[test]
fn mutex_stress_4p_4c() {
    run_stress::<MutexQueue<i32>>(64, 4, 4, VPROD);
}

#[test]
fn mpmc_stress_8p_8c() {
    run_stress::<MpmcQueue<i32>>(64, 8, 8, VPROD);
}

#[test]
fn mutex_stress_8p_8c() {
    run_stress::<MutexQueue<i32>>(64, 8, 8, VPROD);
}

#[test]
fn mpmc_stress_many_consumers() {
    // One producer, many consumers.
    run_stress::<MpmcQueue<i32>>(8, 1, 4, VPROD);
}

#[test]
fn mutex_stress_many_consumers() {
    run_stress::<MutexQueue<i32>>(8, 1, 4, VPROD);
}

#[test]
fn mpmc_stress_many_producers() {
    // Many producers, one consumer.
    run_stress::<MpmcQueue<i32>>(8, 4, 1, VPROD);
}

#[test]
fn mutex_stress_many_producers() {
    run_stress::<MutexQueue<i32>>(8, 4, 1, VPROD);
}

#[test]
fn mpmc_stress_small_capacity_high_contention() {
    // Small capacity forces wraparound and high contention.
    for _ in 0..5 {
        run_stress::<MpmcQueue<i32>>(2, 2, 2, 1000);
    }
}

#[test]
fn mutex_stress_small_capacity_high_contention() {
    for _ in 0..5 {
        run_stress::<MutexQueue<i32>>(2, 2, 2, 1000);
    }
}
