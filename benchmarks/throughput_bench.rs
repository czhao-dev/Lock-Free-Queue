use lfqueue::{BoundedQueue, MpmcQueue, MutexQueue};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

/// All threads spin on `go` until the main thread releases them, so that the
/// timer starts after thread creation overhead.
fn run_throughput<Q>(producers: usize, consumers: usize, ops_per_producer: i64) -> f64
where
    Q: BoundedQueue<i32> + Send + Sync,
{
    let queue = Q::with_capacity(1024);
    let total = producers as i64 * ops_per_producer;
    let consumed = AtomicI64::new(0);
    let ready = AtomicUsize::new(0);
    let go = AtomicBool::new(false);

    let start = thread::scope(|scope| {
        for _ in 0..producers {
            let queue = &queue;
            let ready = &ready;
            let go = &go;
            scope.spawn(move || {
                ready.fetch_add(1, Ordering::Relaxed);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                for _ in 0..ops_per_producer {
                    while queue.push(0).is_err() {}
                }
            });
        }

        for _ in 0..consumers {
            let queue = &queue;
            let ready = &ready;
            let go = &go;
            let consumed = &consumed;
            scope.spawn(move || {
                ready.fetch_add(1, Ordering::Relaxed);
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                while consumed.load(Ordering::Relaxed) < total {
                    if queue.pop().is_some() {
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        while ready.load(Ordering::Acquire) < (producers + consumers) {
            std::hint::spin_loop();
        }
        let start = Instant::now();
        go.store(true, Ordering::Release);
        start
    });

    let seconds = start.elapsed().as_secs_f64();
    total as f64 / seconds / 1e6
}

fn main() {
    println!(
        "# hardware_concurrency={}",
        thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
    );
    println!("threads,queue,total_ops,seconds,mops_per_sec");

    const TOTAL: i64 = 10_000_000;

    for n in [1usize, 2, 4, 8] {
        let ops = TOTAL / n as i64;
        let total = n as i64 * ops;

        let lfq_mops = run_throughput::<MpmcQueue<i32>>(n, n, ops);
        let mtx_mops = run_throughput::<MutexQueue<i32>>(n, n, ops);

        println!(
            "{}+{},lfqueue,{},{:.3},{:.1}",
            n,
            n,
            total,
            total as f64 / (lfq_mops * 1e6),
            lfq_mops
        );
        println!(
            "{}+{},mutex,{},{:.3},{:.1}",
            n,
            n,
            total,
            total as f64 / (mtx_mops * 1e6),
            mtx_mops
        );
    }
}
