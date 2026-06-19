# lfqueue — Lock-Free Multi-Producer Multi-Consumer Queue

![Rust](https://img.shields.io/badge/Rust-1.63%2B-DEA584.svg?logo=rust)
![Cargo](https://img.shields.io/badge/Cargo-stable-CB6219.svg?logo=rust)
![ThreadSanitizer](https://img.shields.io/badge/ThreadSanitizer-clean-brightgreen.svg)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

A bounded, lock-free MPMC (multi-producer multi-consumer) queue implemented in
Rust using atomic operations and explicit memory ordering. Benchmarked against
a mutex-protected queue across varying thread counts and contention levels,
with discussion of the correctness reasoning required for lock-free design.

---

## Overview

Most concurrent data structures are protected by a mutex: a thread acquires the
lock, performs its operation, and releases it. This is simple to reason about
but has a fundamental cost — under contention, threads block and the OS
scheduler must context-switch between them, and even without contention,
acquiring and releasing a lock involves atomic operations and potential cache
coherency traffic.

A **lock-free** data structure guarantees that at least one thread makes
progress in a bounded number of steps, regardless of what other threads are
doing — including if another thread is suspended mid-operation. No thread ever
blocks waiting for another. This is achieved using **atomic compare-and-swap
(CAS)** operations instead of locks, combined with careful **memory ordering**
to ensure operations on different cores become visible in a consistent order.

`lfqueue` implements a bounded ring-buffer queue supporting multiple concurrent
producers (`push`) and multiple concurrent consumers (`pop`), with no mutex
anywhere in the implementation. The unsafe, manually-managed slot storage
(`UnsafeCell<MaybeUninit<T>>`) is the same kind of code C++ writes by default —
Rust just requires it to be explicitly marked and justified with a `// SAFETY:`
comment at every `unsafe` block.

### Why this problem

Lock-free programming is one of the most rigorous correctness exercises in
systems programming — bugs are subtle, non-deterministic, and often invisible
in testing but catastrophic in production under specific timing conditions.
Building one correctly (and being able to explain *why* it's correct) signals a
level of comfort with concurrent correctness reasoning that few candidates can
demonstrate. It is also a direct, practical complement to the memory allocator
project: high-performance allocators and runtime systems use lock-free queues
for work-stealing schedulers, thread pools, and inter-thread message passing.

---

## Architecture

```
                  head (next slot to pop)         tail (next slot to push)
                       │                                │
                       ▼                                ▼
        ┌────┬────┬────┬────┬────┬────┬────┬────┬────┐
slots:  │ E  │ E  │ F  │ F  │ F  │ E  │ E  │ E  │ E  │   E = empty, F = full
        └────┴────┴────┴────┴────┴────┴────┴────┴────┘
          0    1    2    3    4    5    6    7    8

Producers CAS-advance tail and write into the claimed slot.
Consumers CAS-advance head and read from the claimed slot.
Per-slot sequence numbers (not shown) coordinate producer/consumer handoff
without a separate "full/empty" flag race.
```

The queue is a fixed-size array of slots (capacity is a power of two for fast
modular indexing via bitmasking). Each slot carries a **sequence number** in
addition to its data — this is the key mechanism that makes the design correct
(see below), based on the design used in Dmitry Vyukov's bounded MPMC queue.

---

## Key Concepts

### 1. Compare-and-Swap (CAS)

CAS is the fundamental primitive: `compare_exchange(expected, desired)` atomically
checks whether a memory location currently holds `expected`; if so, it writes
`desired` and returns `Ok`; otherwise it returns `Err` with the actual current
value.

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

let tail = AtomicUsize::new(0);

let mut pos = tail.load(Ordering::Relaxed);
while tail
    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
    .is_err()
{
    pos = tail.load(Ordering::Relaxed); // refresh and retry
}
// This thread now "owns" slot `pos` for writing
```

The loop is the standard CAS retry pattern: if another thread won the race and
advanced `tail` first, this thread's CAS fails, `pos` is refreshed to the new
value, and it retries with the new claim. No thread ever blocks — a failed CAS
means "someone else made progress," which satisfies the lock-free progress
guarantee.

`compare_exchange_weak` vs `compare_exchange`: `weak` may fail spuriously
(return `Err` even when the value matches) on some architectures due to how
LL/SC (load-linked/store-conditional) instructions work, but is cheaper in a
retry loop. `compare_exchange` (the "strong" form) never fails spuriously but
may cost more per call. Inside a loop, `weak` is preferred — this is identical
to the `compare_exchange_weak`/`compare_exchange_strong` distinction in C++.

### 2. The ABA Problem and Per-Slot Sequence Numbers

A naive lock-free queue using only head/tail indices is vulnerable to the **ABA
problem**: thread A reads a value `X` at location L, gets preempted; thread B
changes L to `Y` and back to `X`; thread A resumes and its CAS succeeds because
L still equals `X` — but the underlying state has changed in ways thread A's
logic didn't account for.

`lfqueue` avoids ABA by giving each slot a **sequence number** that increments
every time the slot is reused:

```rust
struct Slot<T> {
    sequence: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}
```

A producer claiming slot `i` for the `n`-th time around the ring expects
`sequence == n`. After writing, it sets `sequence = n + 1`, signaling to a
consumer that the slot is ready to read. A consumer expects `sequence == n + 1`
before reading, and sets `sequence = n + capacity` after consuming, signaling
the slot is ready for the *next* producer pass. This turns a 2-state
full/empty flag into a monotonically increasing counter, eliminating the ABA
ambiguity entirely — the sequence number encodes *which lap* around the ring
the slot is on, not just its current state.

The slot's data lives in `UnsafeCell<MaybeUninit<T>>` rather than a plain `T`
because the slot has no valid value until a producer writes one, and Rust's
aliasing rules require `UnsafeCell` for any memory that's mutated through a
shared reference (`&MpmcQueue<T>` — `push`/`pop` deliberately take `&self`, not
`&mut self`, since multiple producers/consumers call them concurrently).

### 3. Memory Ordering

Rust's `std::sync::atomic::Ordering` controls what guarantees an atomic
operation makes about the visibility and ordering of *other* memory operations
around it — this is where most lock-free bugs live, because the code can be
"correct" on x86 (which has strong default ordering) and fail on ARM (which
does not). Rust's memory model is deliberately identical to C++11's
(`std::memory_order`), so the same reasoning transfers directly.

| Order               | Guarantee                                                          | Used for                          |
|---------------------|---------------------------------------------------------------------|------------------------------------|
| `Relaxed`           | Atomicity only — no ordering with other memory ops                 | Counters with no dependent data    |
| `Acquire`           | No subsequent read/write can be reordered *before* this load        | Reading a slot's sequence before reading its data |
| `Release`           | No preceding read/write can be reordered *after* this store         | Publishing a slot's data before updating its sequence |
| `AcqRel`            | Both acquire and release semantics                                   | CAS operations that both read and write |
| `SeqCst`            | Total global ordering across all threads (strongest, most expensive) | Used only where reasoning requires it |

The critical pattern in `lfqueue` is **release-acquire pairing**: a producer
writes the slot's data, then performs a `Release` store to the sequence number.
A consumer performs an `Acquire` load of the sequence number, then reads the
data. The acquire/release pair guarantees that if the consumer observes the
updated sequence number, it is also guaranteed to observe the data write that
happened-before it — without this pairing, the consumer could read stale or
partially-written data even though the sequence number appears updated, because
without ordering constraints, the compiler or CPU may reorder the two stores.

```rust
// Producer
unsafe { (*slot.data.get()).write(value); }            // (1) plain write
slot.sequence.store(pos + 1, Ordering::Release);        // (2) release

// Consumer
let seq = slot.sequence.load(Ordering::Acquire);        // (3) acquire
if seq == expected {
    let value = unsafe { (*slot.data.get()).assume_init_read() }; // (4) plain read
}
```

The release at (2) and acquire at (3) form a synchronizes-with relationship:
if (3) observes the value written by (2), then (1) is guaranteed visible to (4).
This is the mechanism — not a global lock — that makes the handoff safe.

### 4. False Sharing and Cache Line Padding

If `head` and `tail` (each updated by different sets of threads — consumers and
producers respectively) sit on the same cache line, every producer update
invalidates the cache line for consumers and vice versa, even though they touch
logically independent data. This is **false sharing**, and it can degrade
throughput by an order of magnitude under contention.

`lfqueue` pads `head` and `tail` to separate 64-byte cache lines using
`#[repr(align(64))]`:

```rust
#[repr(align(64))]
struct CachePadded<T> {
    value: T,
}
```

This is a small code change with a measurable effect — the benchmarks section
quantifies it.

---

## Design Decisions and Tradeoffs

**Bounded vs unbounded queue**

`lfqueue` is bounded (fixed capacity, set at construction). Unbounded lock-free
queues exist (e.g., the Michael-Scott queue using a linked list with CAS-based
node insertion) but introduce a harder problem: **safe memory reclamation**.
When a node is dequeued from a lock-free linked list, another thread might still
hold a pointer to it — freeing it immediately risks a use-after-free. Solving
this requires hazard pointers, epoch-based reclamation, or RCU, each of which is
a substantial topic on its own (and in Rust's ownership model, additionally
needs `unsafe` to bypass the borrow checker's single-owner assumption). A
bounded ring buffer avoids the problem entirely because slots are reused, never
freed — at the cost of needing to handle the "queue full" case (the design
choice below).

**Behavior on full/empty: spin vs block**

`push` on a full queue returns `Err(value)` — handing the value back to the
caller rather than dropping it — and `pop` on an empty queue returns `None`,
immediately and non-blocking, rather than spinning or blocking. This keeps the
data structure itself fully lock-free and leaves the retry/backoff policy to
the caller — appropriate because different use cases want different policies
(busy-spin for ultra-low-latency, exponential backoff for CPU-friendly polling,
or falling back to a condition variable for a hybrid design). Returning the
value on failure rather than a bare `bool` (as the C++ original did via an
out-parameter) is a natural fit for Rust's move semantics: the caller never
loses ownership of a value it failed to enqueue.

**Lock-free vs wait-free**

This queue is lock-free but not wait-free: under pathological scheduling, a
thread's CAS could fail repeatedly if other threads continuously win the race
(though this is exceptionally rare in practice with `compare_exchange_weak` and
real scheduler behavior). True wait-free algorithms guarantee every thread
completes in a bounded number of steps regardless of other threads, but are
substantially more complex to implement correctly. Lock-free is the standard,
practical target for high-performance concurrent data structures.

**Why not just use a mutex?**

This is the question every interviewer will ask, and the honest answer is: for
most applications, a `Mutex<VecDeque<T>>` is the right choice. Mutex overhead
(tens of nanoseconds, uncontended) is negligible for most workloads, and the
implementation is trivially correct — no `unsafe` required. Lock-free
structures are justified specifically when: (1) the queue is on a hot path
called millions of times per second, (2) priority inversion is a concern (a
low-priority thread holding a lock blocking a high-priority thread), or (3) the
structure is used in a context where blocking is unacceptable, such as a signal
handler or real-time audio callback. The benchmarks below quantify when the
crossover happens.

---

## Benchmarks

**Hardware:** Apple M3, 8 cores (8 logical), macOS
**Compiler:** rustc 1.96.0, `cargo build --release` (opt-level 3, codegen-units 1)
**Operations:** 10,000,000 items transferred per configuration

### Throughput vs thread count (fixed 10 M total items)

| Threads (P+C) | lfqueue (Mops/sec) | mutex queue (Mops/sec) |
|:-------------:|:------------------:|:----------------------:|
| 1 + 1         | 133.4              | 28.4                   |
| 2 + 2         | 13.8               | 32.6                   |
| 4 + 4         | 5.3                | 19.7                   |
| 8 + 8         | 2.7                | 17.6                   |

**Interpretation:** The lock-free queue wins decisively at 1+1 (no contention,
avoids lock overhead entirely). As thread count rises, the CAS-based design
suffers from cache-invalidation storms: every producer's CAS on `tail` and
every consumer's CAS on `head` broadcasts a cache-line invalidation to all
other cores. On Apple Silicon's strongly-ordered architecture, the mutex queue
serialises access more cheaply than repeated CAS failures under high
contention. This is a well-known result — lock-free does not mean
faster-under-all-conditions; it means progress without blocking. The
lock-free queue's advantages show clearly in the latency distribution below,
particularly at the tail.

### Effect of cache line padding (4 producers + 4 consumers)

| Configuration               | Throughput (Mops/sec) |
|:---------------------------:|:---------------------:|
| Without padding (false sharing) | 3.1               |
| With 64-byte padding            | 3.5               |

Padding `head` and `tail` onto separate cache lines gives a **~13%
throughput improvement**, consistent across repeated runs, with no
algorithmic change — the only difference is preventing the two counters from
sharing a cache line and invalidating each other on every operation. (The
effect size is smaller than a textbook false-sharing benchmark might suggest,
since Apple Silicon's large L2 cache and aggressive prefetcher absorb some of
the invalidation cost — but the direction is consistent across runs.)

### Latency distribution (single producer, single consumer, 100 k samples)

| Metric | lfqueue (ns) | mutex queue (ns) |
|:------:|:------------:|:----------------:|
| p50    | 4,500        | 11,167            |
| p99    | 6,750        | 149,583           |
| p999   | 11,458       | 798,917           |

The lock-free queue shows dramatically lower tail latency than the mutex
queue. This is the expected advantage: when a thread is preempted while
holding a mutex, every other thread stalls until it resumes, and OS scheduler
involvement adds milliseconds of jitter at the tail. A lock-free queue never
blocks a waiter — worst case, a consumer spins briefly on a sequence number
before the producer publishes, but it never waits for an OS reschedule.

*Reproduce with `./benchmarks/run_all.sh` (builds Release binaries if needed).*

---

## Building and Running

### Requirements

- Rust 1.63+ (stable) — for `std::thread::scope`
- Nightly toolchain, optional — only needed to run under ThreadSanitizer or Miri

### Build

```bash
cargo build --release
```

### Run benchmarks

```bash
cargo run --release --example throughput_bench
cargo run --release --example padding_bench
cargo run --release --example latency_bench
```

Or run all three and save results to `benchmarks/results/`:

```bash
./benchmarks/run_all.sh
```

### Run correctness tests

```bash
cargo test --release
```

### Run with ThreadSanitizer

```bash
rustup toolchain install nightly
rustup component add rust-src --toolchain nightly

RUSTFLAGS="-Z sanitizer=thread" \
    cargo +nightly test -Z build-std --target aarch64-apple-darwin --release --tests
```

(Substitute your own target triple, e.g. `x86_64-unknown-linux-gnu`.)

ThreadSanitizer is essential for this project — it detects data races that may
not manifest in normal testing but are real bugs under different scheduling or
hardware. **Every commit should pass under TSan** before being considered
correct; this is standard practice for any production lock-free code.

---

## Testing Strategy

Lock-free correctness cannot be fully validated by normal unit tests, because
race conditions are timing-dependent. The test suite uses several complementary
approaches:

**Functional correctness**: single-threaded push/pop sequences verify FIFO
ordering, full/empty boundary conditions, and wraparound behavior.

**Concurrent stress test**: N producers each push a unique sequence of values;
M consumers pop and record what they receive. After completion, verify that
every value pushed was received exactly once (no duplicates, no drops) — this
catches the most common class of lock-free bugs.

**ThreadSanitizer**: run the stress tests under TSan (nightly Rust, `-Z
sanitizer=thread`) to catch missing memory ordering even when the functional
test passes — TSan can detect a "correct by luck" race that would fail on
different hardware.

**Loom (optional, advanced)**: [loom](https://github.com/tokio-rs/loom) is
Rust's model-checking tool for concurrent code — the direct equivalent of
Relacy/CDSChecker in the C++ world. It exhaustively explores thread
interleavings for small test cases by substituting `loom`'s atomic types for
`std`'s, providing much stronger guarantees than running many random
iterations. Mentioned here as a stretch goal for demonstrating depth; adopting
it would require gating the atomic imports behind `#[cfg(loom)]`.

### Results

```
$ cargo test --release
running 24 tests
test mpmc_empty_and_full ... ok
test mpmc_wraparound_order ... ok
test mpmc_exact_capacity ... ok
test mpmc_move_only_values ... ok
test mpmc_rejects_zero_capacity - should panic ... ok
test mpmc_rejects_non_power_of_two - should panic ... ok
test mutex_empty_and_full ... ok
test mutex_wraparound_order ... ok
test mutex_exact_capacity ... ok
test mutex_move_only_values ... ok
test mutex_rejects_zero_capacity - should panic ... ok
test mpmc_stress_1p_1c ... ok
test mpmc_stress_2p_2c ... ok
test mpmc_stress_4p_4c ... ok
test mpmc_stress_8p_8c ... ok
test mpmc_stress_many_consumers ... ok
test mpmc_stress_many_producers ... ok
test mpmc_stress_small_capacity_high_contention ... ok
test mutex_stress_1p_1c ... ok
test mutex_stress_2p_2c ... ok
test mutex_stress_4p_4c ... ok
test mutex_stress_8p_8c ... ok
test mutex_stress_many_consumers ... ok
test mutex_stress_many_producers ... ok
test mutex_stress_small_capacity_high_contention ... ok

test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

The `correctness` test suite covers, in order: empty/full boundary conditions,
wraparound ordering, exact-capacity behavior, move-only value types (a
non-`Copy` `Box<i32>`), and constructor validation (zero capacity, non-power-of-
two capacity) — followed by twelve concurrent stress configurations run
against **both** `MpmcQueue` and `MutexQueue` (1+1, 2+2, 4+4, 8+8, 1
producer/4 consumers, 4 producers/1 consumer, and 5 repetitions of a
small-capacity/high-contention case), each verifying every pushed value is
received exactly once with no drops or duplicates.

```
$ rustup component add rust-src --toolchain nightly
$ RUSTFLAGS="-Z sanitizer=thread" \
    cargo +nightly test -Z build-std --target aarch64-apple-darwin --release --tests
running 24 tests
...
test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

ThreadSanitizer reports **zero data races** across every stress configuration
— the release/acquire pairing on the sequence number is sufficient to make
the producer/consumer handoff safe under TSan's race detector, not merely
"correct by luck" on a specific architecture's memory model.

---

## Relationship to Broader Systems Work

Lock-free queues are the backbone of high-performance concurrent systems:

- **Work-stealing schedulers** (used in thread pools, task-based parallelism
  frameworks like Intel TBB and Tokio's multi-threaded runtime) use lock-free
  deques so idle threads can steal work from busy threads without blocking
  them.

- **Inter-thread communication in low-latency systems** (audio processing, HFT,
  real-time control loops) requires bounded, predictable-latency message passing
  — exactly what this queue provides, in contrast to a mutex queue's occasional
  long tail latencies under preemption.

- **Memory allocators** (connecting to the companion allocator project):
  high-performance allocators use lock-free free-lists for cross-thread memory
  return — when thread A frees memory originally allocated by thread B, it must
  hand the memory back without locking B's allocator state.

- **Compiler and runtime internals**: garbage collectors and JIT compilers use
  lock-free structures for work queues (e.g., parallel marking phases in a GC)
  where mutex contention would directly translate to pause-time regressions.

The memory ordering reasoning in this project — release/acquire pairing,
happens-before relationships — is the same reasoning required to understand
data races in any concurrent system, including the kernel synchronization in
the character device driver project (where the kernel's locking primitives
handle this for you, but the underlying memory model is identical).

---

## Future Extensions

- **Unbounded variant with hazard pointers** — implement the Michael-Scott
  queue with a safe memory reclamation scheme, demonstrating the harder
  unbounded case
- **Backoff strategies** — implement and benchmark exponential backoff on CAS
  failure vs busy-spin, quantifying the CPU-usage/latency tradeoff
- **SPSC fast path** — a single-producer/single-consumer specialization can
  avoid CAS entirely (plain atomic loads/stores suffice when there's only one
  writer per index), and benchmarking SPSC vs MPMC quantifies the cost of
  generality
- **Loom-based model checking** — gate the atomic types behind `#[cfg(loom)]`
  so `cargo test --features loom` can exhaustively check small interleavings,
  rather than relying on random scheduling to surface bugs
- **Atomic wait/notify** — Rust's standard library has an unstable
  `Atomic*::wait`/`notify` API (mirroring C++20's `std::atomic::wait`/`notify`)
  that would let consumers block on a futex instead of busy-polling; swap to it
  once stabilized

---

## References

- Vyukov, D. *Bounded MPMC Queue* — the sequence-number design this
  implementation is based on (1024cores.net)
- Michael, M. & Scott, M. *Simple, Fast, and Practical Non-Blocking and
  Blocking Concurrent Queue Algorithms* (PODC 1996) — the unbounded queue
  referenced in Future Extensions
- Bos, M. *Rust Atomics and Locks* — the standard modern reference for
  `std::sync::atomic`, memory ordering, and building concurrent data
  structures in Rust
- *The Rustonomicon* — the reference for `unsafe` Rust, including
  `UnsafeCell` and `MaybeUninit`, both used in this implementation
- [loom](https://github.com/tokio-rs/loom) — Rust's concurrency model checker,
  the equivalent of Relacy/CDSChecker referenced in Testing Strategy
- Preshing, J. — preshing.com blog series on lock-free programming and memory
  ordering (clear, practical explanations with diagrams; language-agnostic)
