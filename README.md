# fxmatch — HFT Limit Order Book & Matching Engine

![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C.svg?logo=cplusplus)
![CMake](https://img.shields.io/badge/CMake-3.16%2B-064F8C.svg?logo=cmake)
![ThreadSanitizer](https://img.shields.io/badge/ThreadSanitizer-clean-brightgreen.svg)
![Tests](https://img.shields.io/badge/tests-36%20passing-brightgreen.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

A simulation of the core inter-thread pipeline inside an HFT matching engine,
written in C++17 with zero external dependencies. A synthetic feed generates
limit orders, an order book matches them by price-time priority, and
end-to-end order-to-fill latency is measured through the pipeline.

The **lock-free MPMC queue** is the IPC primitive at every pipeline boundary.
It is the reason raw p50 latency is ~5.6µs and p999 stays under 32µs, versus
~8µs p50 and ~53µs p999 for a mutex-based queue — the lock-free queue never
stalls on OS scheduling, so no thread waits for another thread's lock release.
Under matching-engine load (100k orders), the MPMC pipeline shows **17× lower
p50** and **28× lower p99** than a mutex pipeline.

---

## Architecture

```
 FeedSimulator thread(s)
        │
        │  MPMCQueue<Order>   ← lock-free ring buffer; never blocks; zero allocation
        ▼
 MatchingEngine thread
   (owns all per-symbol OrderBooks — thread confinement, no locks on the book)
        │
        │  MPMCQueue<Fill>
        ▼
 Reporter thread
   (accumulates fill latencies; prints p50/p99/p999 at shutdown)
```

The order book is **not** lock-free. Thread confinement — only the
MatchingEngine thread ever touches the books — is the correct architectural
choice here: a lock-free `std::map` equivalent would require hazard pointers
or epoch-based reclamation. By routing all orders through `MPMCQueue<Order>`,
the engine receives them sequentially and can use a plain `std::map` without
any synchronization. The queue absorbs the concurrency so the book doesn't have
to; both ends stay simple.

---

## Domain Model

| Type | Fields | Notes |
|------|--------|-------|
| `Order` | `id, symbol, side, price, quantity, timestamp_ns` | Trivially copyable; fits in registers |
| `Fill` | `buy_order_id, sell_order_id, symbol, price, quantity, submit_ns, fill_ns` | `fill_ns - submit_ns` = order-to-fill latency |
| `Side` | `Bid \| Ask` | `uint8_t` enum |
| `Symbol` | `std::array<char, 8>` | Fixed-size, no heap alloc, hashable |

`price` is stored as `uint64_t` integer ticks (cents, basis points, etc.) — never
`double`. Floating-point arithmetic is non-associative: the same computation
can produce different results in a different evaluation order, which would break
the deterministic price-time priority required for a correct matching engine.
Integer fixed-point is deterministic and hardware-accelerated on all modern ISAs.

---

## Order Book Design

Each symbol has an independent `OrderBook`. Price-time priority (FIFO within a
level) is the exchange-standard rule: the best price executes first; within the
same price, earlier orders execute first.

```
Bids: std::map<price, std::deque<Order>, std::greater<>>
          highest price → begin()   (best bid)

Asks: std::map<price, std::deque<Order>>
          lowest price → begin()    (best ask)
```

`std::deque<Order>` at each price level gives O(1) push-back (new passive
orders) and O(1) pop-front (consuming the oldest resting order at that level),
preserving FIFO ordering.

**`match_order` algorithm:**

1. Aggressor buy: iterate asks from `begin()` upward while `ask_price ≤ bid_price`.
   Pop the front order from each level (FIFO), compute `fill_qty = min(incoming.qty, resting.qty)`,
   emit a `Fill`. Decrement quantities. Remove the level if empty.
2. Aggressor sell: mirror — iterate bids from `begin()` downward while `bid_price ≥ ask_price`.
3. Post any unfilled remainder to the passive side of the book.

---

## Lock-Free Queue: Why It Matters Here

### 1. Compare-and-Swap (CAS)

The MPMC queue advances `head` and `tail` with atomic compare-and-swap rather
than a mutex. A producer claims a slot by CAS-advancing `tail`; a consumer
claims a slot by CAS-advancing `head`. No thread ever blocks waiting for
another — a failed CAS means "someone else made progress," which satisfies
the lock-free progress guarantee.

```cpp
std::size_t pos = tail_.value.load(std::memory_order_relaxed);
for (;;) {
    auto& slot = buffer_[pos & mask_];
    std::size_t seq = slot.sequence.load(std::memory_order_acquire);
    auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
    if (diff == 0) {
        if (tail_.value.compare_exchange_weak(pos, pos + 1, ...)) break;
    } else if (diff < 0) {
        return false; // queue full
    } else {
        pos = tail_.value.load(std::memory_order_relaxed); // retry
    }
}
```

### 2. The ABA Problem and Per-Slot Sequence Numbers

A naive lock-free queue using only head/tail indices is vulnerable to the ABA
problem: thread A reads slot 0 as "empty," gets preempted, thread B fills and
empties slot 0 (ABA), thread A resumes and incorrectly treats a now-full slot
as empty.

`fxmatch`'s queue uses a per-slot **sequence number** (based on Dmitry Vyukov's
design) that increments each time a slot is reused. A producer expects
`seq == pos` (slot ready for write); after writing it sets `seq = pos + 1`.
A consumer expects `seq == pos + 1` (data ready); after reading it sets
`seq = pos + capacity`. The monotonically increasing counter encodes *which
lap* around the ring a slot is on, eliminating ABA entirely.

### 3. Memory Ordering

| Ordering | Used for |
|----------|----------|
| `Relaxed` | Initial head/tail loads; CAS attempts |
| `Acquire` | Reading slot sequence before reading slot data |
| `Release` | Writing slot sequence after writing slot data |

The release at the producer and acquire at the consumer form a
*synchronizes-with* relationship: if the consumer observes the updated sequence
number, it is guaranteed to also observe the data write that happened-before
it — without this pairing, the consumer could read stale data on weakly-ordered
architectures (ARM, RISC-V) even after seeing the sequence update.

### 4. False Sharing and Cache Line Padding

`head_` and `tail_` are owned by different thread sets (consumers/producers).
If they share a cache line, every CAS invalidates it for all other cores even
though they access logically independent data. `fxmatch` pads both to 64-byte
lines with `alignas(64)`:

```cpp
struct alignas(64) PaddedAtomic { std::atomic<size_t> value{0}; };
```

Effect at 4+4 threads, 10 M ops:

| Configuration | Throughput (Mops/sec) |
|:-------------:|:---------------------:|
| Without padding (false sharing) | 3.3 |
| With 64-byte padding | 5.0 |

**52% throughput improvement** from one structural change.

---

## Benchmarks

**Hardware:** Apple M3, 8 cores, macOS · **Compiler:** Apple Clang 21, `-O3`

### Raw Queue Throughput (N producers + N consumers, 10 M items)

| Threads (P+C) | fxmatch MPMC (Mops/sec) | Mutex (Mops/sec) |
|:-------------:|:-----------------------:|:----------------:|
| 1 + 1         | 53.3                    | 10.8             |
| 2 + 2         | 13.8                    | 24.9             |
| 4 + 4         | 3.7                     | 16.9             |
| 8 + 8         | 1.8                     | 16.6             |

Lock-free wins at 1+1 (no contention, no lock overhead). Under high contention,
repeated CAS failures and cache-invalidation storms outweigh the benefit of not
blocking — the mutex serialises access more cheaply. Lock-free does not mean
faster-under-all-conditions; it means progress without blocking.

### Raw Queue Latency (1 producer, 1 consumer, 100k samples)

| Metric | fxmatch MPMC (ns) | Mutex (ns) |
|:------:|:-----------------:|:----------:|
| p50    | 5,583             | 8,042      |
| p99    | 7,042             | 24,458     |
| p999   | 31,666            | 52,667     |

Lock-free delivers **1.7× better p999**. When a thread is preempted while
holding a mutex, every waiter stalls for the duration of the OS reschedule
(10–100 µs on Linux). A lock-free queue never blocks a waiter — worst case,
a consumer retries a CAS briefly, but never waits for an OS reschedule.

### End-to-End Order-to-Fill Latency (MatchingEngine pipeline, 100k orders)

Measures `fill_ns − submit_ns`: from when the Order enters the pipeline to
when the MatchingEngine emits the Fill — the canonical HFT tick-to-trade metric.

| Pipeline              | p50 (ns)   | p99 (ns)    | p999 (ns)   |
|:---------------------:|:----------:|:-----------:|:-----------:|
| MPMCQueue (lock-free) | 959,875    | 1,177,709   | 1,191,667   |
| MutexQueue            | 16,871,209 | 32,982,625  | 34,138,541  |

**17× lower p50 · 28× lower p99** for the lock-free pipeline. Under sustained
matching-engine load the mutex pipeline stalls every consumer behind a lock-
holding thread that may be preempted by the OS scheduler. The lock-free pipeline
eliminates this stall: no thread ever blocks waiting for another thread's lock.

*Reproduce:*
```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel
./build/benchmarks/match_bench       # pipeline latency
./build/benchmarks/latency_bench     # raw queue latency
./build/benchmarks/throughput_bench  # throughput vs thread count
./build/benchmarks/padding_bench     # false-sharing effect
```

---

## Building and Running

### Requirements

- C++17 compiler (GCC 9+, Clang 10+, MSVC 19.14+)
- CMake 3.16+

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

### Run the live simulation

```bash
./build/examples/fxmatch_sim --symbols AAPL,MSFT,TSLA --secs 5
```

### Run correctness tests

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build --parallel
ctest --test-dir build -V
```

### Run with ThreadSanitizer

```bash
cmake -B build-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan --parallel
ctest --test-dir build-tsan -V
```

ThreadSanitizer reports **zero data races** across all stress configurations.

---

## Testing

**36 tests across two suites:**

### `correctness_test` (24 tests) — queue primitive

Boundary conditions, wraparound ordering, move-only values, constructor
validation, and 12 concurrent stress configurations (1+1 through 8+8 threads,
asymmetric 1P/4C and 4P/1C, high-contention small-capacity) — each verifying
every pushed value is received exactly once with no drops or duplicates, run
against both `MPMCQueue` and `MutexQueue`.

### `engine_test` (12 tests) — domain correctness

**OrderBook (8 tests):** empty-book, no-cross spread, exact fill, partial fill
aggressor, partial fill resting, price-time priority across levels, FIFO within
a level, fill timestamps.

**Pipeline integration (4 tests):** engine produces correct fills; concurrent
feed + engine for 100ms; MPMCQueue and MutexQueue produce identical fill counts
from the same input; clean shutdown (all threads join within 2s, no deadlock).

---

## Design Decisions

**Non-blocking push/pop**

`MPMCQueue` returns `false` on push-when-full and `false` on pop-when-empty.
Leaving retry/backoff to the caller is intentional: different callers need
different policies (busy-spin for ultra-low latency, exponential backoff for
CPU efficiency), and the queue's job is to be correct and fast, not to
prescribe a policy.

**Lock-free but not wait-free**

Under pathological scheduling, a single CAS could retry repeatedly if other
threads continuously win the race — but this is rare in practice with
`compare_exchange_weak` and real schedulers. True wait-freedom (every thread
completes in bounded steps regardless of others) is substantially more complex
and not justified here.

**Why is the order book not lock-free?**

Thread confinement is simpler and equally correct. A lock-free `std::map`
requires node-level CAS and a safe memory reclamation scheme (hazard pointers,
epoch-based reclamation, or RCU). By routing orders through `MPMCQueue<Order>`,
the engine receives them sequentially and needs no synchronization on the book
itself. The architecture shows that lock-freedom and simplicity are not in
opposition: the queue handles concurrency so the book doesn't have to.

**Integer prices, not float**

`price` is `uint64_t` ticks throughout — never `double`. Floating-point
arithmetic is non-associative: the same order sequence could produce different
fill prices depending on evaluation order or compiler optimization. Integer
arithmetic is deterministic and faster on all modern ISAs.

**Fixed-size Symbol**

`Symbol = std::array<char, 8>` is trivially copyable, hashable, and fits in a
register pair. Passing a ticker through a lock-free queue as `std::string`
would require heap allocation on every push — a 50–100 ns stall on the hot path.

---

## Future Extensions

- **CPU affinity**: pin the matching engine thread to a dedicated core
  (`pthread_setaffinity_np` on Linux) to eliminate scheduler jitter
- **Per-symbol engine sharding**: consistent hash on `Symbol` to route each
  ticker to its own matching thread, eliminating the single-engine bottleneck
  for many-symbol workloads
- **SPSC fast path**: for a single feed / single engine topology, SPSC can skip
  CAS entirely (plain atomic load/store suffice), quantifying the cost of MPMC
  generality
- **Loom / CDSChecker model checking**: gate atomics behind a mock atomic type
  to exhaustively enumerate thread interleavings for small test cases
- **HDR histogram**: implement a lock-free histogram recorder (std-only) to
  capture the full latency distribution without sorting millions of samples

---

## References

- Vyukov, D. *Bounded MPMC Queue* — the sequence-number design this
  implementation is based on (1024cores.net)
- Michael, M. & Scott, M. *Simple, Fast, and Practical Non-Blocking and
  Blocking Concurrent Queue Algorithms* (PODC 1996)
- Preshing, J. — preshing.com series on lock-free programming and memory
  ordering (language-agnostic, with diagrams)
- *ISO C++11 Memory Model* — the formal specification of `std::memory_order`
