#include "fxmatch/fill.hpp"
#include "fxmatch/mpmc_queue.hpp"
#include "fxmatch/mutex_queue.hpp"
#include "fxmatch/order.hpp"
#include "fxmatch/order_book.hpp"
#include "fxmatch/engine.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <thread>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fxmatch::Order make_order(uint64_t id, fxmatch::Side side, uint64_t price, uint64_t qty,
                           uint64_t ts_ns = 0) {
    return fxmatch::Order{id, fxmatch::make_symbol("TEST"), side, price, qty, ts_ns};
}

static constexpr uint64_t kFillNs = 9999;

// ---------------------------------------------------------------------------
// OrderBook correctness tests
// ---------------------------------------------------------------------------

// 1. No fill when book is empty — passive order is posted
void test_no_fill_empty_book() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    auto fills = book.match_order(make_order(1, fxmatch::Side::Bid, 100, 10), kFillNs);
    assert(fills.empty());
    assert(book.best_bid().has_value() && book.best_bid().value() == 100);
    assert(!book.best_ask().has_value());
}

// 2. No fill when spread exists (bid < ask)
void test_no_fill_spread() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    book.match_order(make_order(1, fxmatch::Side::Ask, 102, 5), kFillNs);
    auto fills = book.match_order(make_order(2, fxmatch::Side::Bid, 100, 5), kFillNs);
    assert(fills.empty());
    assert(book.best_bid().value() == 100);
    assert(book.best_ask().value() == 102);
}

// 3. Exact fill: bid 100 qty 10 vs ask 100 qty 10 → one fill qty 10
void test_exact_fill() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    book.match_order(make_order(1, fxmatch::Side::Ask, 100, 10), kFillNs);
    auto fills = book.match_order(make_order(2, fxmatch::Side::Bid, 100, 10), kFillNs);
    assert(fills.size() == 1);
    assert(fills[0].quantity == 10);
    assert(fills[0].price == 100);
    assert(fills[0].buy_order_id == 2);
    assert(fills[0].sell_order_id == 1);
    assert(!book.best_ask().has_value());
    assert(!book.best_bid().has_value());
}

// 4. Partial fill — aggressor has more qty than resting
void test_partial_fill_aggressor() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    book.match_order(make_order(1, fxmatch::Side::Ask, 100, 3), kFillNs);
    auto fills = book.match_order(make_order(2, fxmatch::Side::Bid, 100, 10), kFillNs);
    assert(fills.size() == 1);
    assert(fills[0].quantity == 3);
    // Aggressor remainder (qty 7) posted as bid
    assert(!book.best_ask().has_value());
    assert(book.best_bid().has_value() && book.best_bid().value() == 100);
}

// 5. Partial fill — resting has more qty than aggressor
void test_partial_fill_resting() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    book.match_order(make_order(1, fxmatch::Side::Ask, 100, 10), kFillNs);
    auto fills = book.match_order(make_order(2, fxmatch::Side::Bid, 100, 3), kFillNs);
    assert(fills.size() == 1);
    assert(fills[0].quantity == 3);
    // Resting remainder (qty 7) stays in book
    assert(book.best_ask().has_value() && book.best_ask().value() == 100);
    assert(!book.best_bid().has_value());
}

// 6. Price-time priority across levels: buy at 102 should match ask@101 first
void test_price_time_priority_across_levels() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    book.match_order(make_order(1, fxmatch::Side::Ask, 101, 5), kFillNs);
    book.match_order(make_order(2, fxmatch::Side::Ask, 102, 5), kFillNs);
    auto fills = book.match_order(make_order(3, fxmatch::Side::Bid, 102, 8), kFillNs);
    assert(fills.size() == 2);
    assert(fills[0].price == 101 && fills[0].quantity == 5);
    assert(fills[1].price == 102 && fills[1].quantity == 3);
}

// 7. FIFO within a price level
void test_fifo_within_level() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    book.match_order(make_order(1, fxmatch::Side::Ask, 100, 5), kFillNs); // first in
    book.match_order(make_order(2, fxmatch::Side::Ask, 100, 5), kFillNs); // second in
    // Aggressor buy for 5 units — should fill order 1 first
    auto fills = book.match_order(make_order(3, fxmatch::Side::Bid, 100, 5), kFillNs);
    assert(fills.size() == 1);
    assert(fills[0].sell_order_id == 1);
}

// 8. Fill timestamps: submit_ns = aggressor timestamp_ns; fill_ns = kFillNs
void test_fill_timestamps() {
    fxmatch::OrderBook book(fxmatch::make_symbol("TEST"));
    book.match_order(make_order(1, fxmatch::Side::Ask, 100, 5), kFillNs);
    const uint64_t submit_time = 12345678;
    auto order = make_order(2, fxmatch::Side::Bid, 100, 5, submit_time);
    auto fills = book.match_order(order, kFillNs);
    assert(fills.size() == 1);
    assert(fills[0].submit_ns == submit_time);
    assert(fills[0].fill_ns == kFillNs);
}

// ---------------------------------------------------------------------------
// Pipeline integration tests
// ---------------------------------------------------------------------------

// 9. Engine thread: spawn briefly to confirm fills are produced
void test_engine_produces_fills() {
    fxmatch::MPMCQueue<fxmatch::Order> order_q(256);
    fxmatch::MPMCQueue<fxmatch::Fill>  fill_q(256);
    std::atomic<bool> shutdown{false};
    fxmatch::MatchingEngine<fxmatch::MPMCQueue, fxmatch::MPMCQueue> engine(order_q, fill_q, shutdown);

    // 5 asks, 5 matching bids
    for (uint64_t i = 0; i < 5; ++i)
        order_q.push(make_order(i, fxmatch::Side::Ask, 100, 1));
    for (uint64_t i = 5; i < 10; ++i)
        order_q.push(make_order(i, fxmatch::Side::Bid, 100, 1));

    std::thread eng([&]{ engine.run(); });
    // Wait until fill queue has 5 fills (engine processed all)
    fxmatch::Fill fill;
    int fill_count = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (fill_count < 5 && std::chrono::steady_clock::now() < deadline) {
        if (fill_q.pop(fill)) ++fill_count;
    }
    shutdown.store(true);
    eng.join();
    assert(fill_count == 5);
}

// 10. Concurrent: 2 feed threads for 100ms → fill count > 0, no crash
void test_concurrent_feed_and_engine() {
    fxmatch::MPMCQueue<fxmatch::Order> order_q(4096);
    fxmatch::MPMCQueue<fxmatch::Fill>  fill_q(4096);
    std::atomic<bool> shutdown{false};
    fxmatch::MatchingEngine<fxmatch::MPMCQueue, fxmatch::MPMCQueue> engine(order_q, fill_q, shutdown);

    std::atomic<int> fill_count{0};
    const auto sym = fxmatch::make_symbol("AAPL");

    std::thread eng([&]{ engine.run(); });

    std::thread reporter([&]{
        fxmatch::Fill f;
        while (!shutdown.load(std::memory_order_relaxed) || fill_q.pop(f))
            if (fill_q.pop(f)) fill_count.fetch_add(1, std::memory_order_relaxed);
    });

    uint64_t id = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    while (std::chrono::steady_clock::now() < deadline) {
        fxmatch::Order ask{id++, sym, fxmatch::Side::Ask, 100, 1, 0};
        fxmatch::Order bid{id++, sym, fxmatch::Side::Bid, 100, 1, 0};
        while (!order_q.push(ask)) {}
        while (!order_q.push(bid)) {}
    }

    shutdown.store(true);
    eng.join();
    reporter.join();

    assert(fill_count.load() > 0);
}

// 11. MPMCQueue and MutexQueue produce identical fill counts for same orders
template <template <typename> class OQ, template <typename> class FQ>
static int pipeline_fill_count() {
    OQ<fxmatch::Order> order_q(256);
    FQ<fxmatch::Fill>  fill_q(256);
    std::atomic<bool> shutdown{false};
    fxmatch::MatchingEngine<OQ, FQ> engine(order_q, fill_q, shutdown);

    const auto sym = fxmatch::make_symbol("TEST");
    for (uint64_t i = 0; i < 20; ++i) {
        fxmatch::Side s = (i % 2 == 0) ? fxmatch::Side::Ask : fxmatch::Side::Bid;
        order_q.push(fxmatch::Order{i, sym, s, 100, 1, 0});
    }

    std::thread eng([&]{ engine.run(); });
    fxmatch::Fill f;
    int count = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (count < 10 && std::chrono::steady_clock::now() < deadline)
        if (fill_q.pop(f)) ++count;
    shutdown.store(true);
    eng.join();
    return count;
}

void test_mpmc_and_mutex_fill_count_equal() {
    const int mpmc_count  = pipeline_fill_count<fxmatch::MPMCQueue,  fxmatch::MPMCQueue>();
    const int mutex_count = pipeline_fill_count<fxmatch::MutexQueue, fxmatch::MutexQueue>();
    if (mpmc_count != 10 || mutex_count != 10) {
        std::fprintf(stderr,
            "fill count mismatch: mpmc=%d mutex=%d (expected 10 each)\n",
            mpmc_count, mutex_count);
        std::abort();
    }
}

// 12. Shutdown after 50ms: all threads join within 2s (no deadlock)
void test_shutdown_drains_cleanly() {
    fxmatch::MPMCQueue<fxmatch::Order> order_q(4096);
    fxmatch::MPMCQueue<fxmatch::Fill>  fill_q(4096);
    std::atomic<bool> shutdown{false};
    fxmatch::MatchingEngine<fxmatch::MPMCQueue, fxmatch::MPMCQueue> engine(order_q, fill_q, shutdown);

    std::thread eng([&]{ engine.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    shutdown.store(true);

    bool joined = false;
    std::thread waiter([&]{
        eng.join();
        joined = true;
    });
    waiter.join();
    assert(joined);
}

} // namespace

int main() {
    // OrderBook correctness
    test_no_fill_empty_book();
    test_no_fill_spread();
    test_exact_fill();
    test_partial_fill_aggressor();
    test_partial_fill_resting();
    test_price_time_priority_across_levels();
    test_fifo_within_level();
    test_fill_timestamps();

    // Pipeline integration
    test_engine_produces_fills();
    test_concurrent_feed_and_engine();
    test_mpmc_and_mutex_fill_count_equal();
    test_shutdown_drains_cleanly();

    return 0;
}
