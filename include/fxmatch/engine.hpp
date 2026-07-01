#ifndef FXMATCH_ENGINE_HPP
#define FXMATCH_ENGINE_HPP

#include <atomic>
#include <cstdint>
#include <thread>
#include <unordered_map>
#include "fxmatch/clock.hpp"
#include "fxmatch/fill.hpp"
#include "fxmatch/mpmc_queue.hpp"
#include "fxmatch/order.hpp"
#include "fxmatch/order_book.hpp"

namespace fxmatch {

// FNV-1a hash for Symbol (std::array<char, 8>).
// Required because std::unordered_map has no built-in hash for std::array.
struct SymbolHash {
    std::size_t operator()(const Symbol& s) const noexcept {
        std::size_t h = 14695981039346656037ULL;
        for (char c : s) {
            h ^= static_cast<uint8_t>(c);
            h *= 1099511628211ULL;
        }
        return h;
    }
};

// Single-threaded matching engine that drains an order queue and emits fills.
//
// Architecture:
//   FeedSimulator(s) → order_rx [OQ] → MatchingEngine → fill_tx [FQ]
//
// Templatised over the queue types so that benchmarks can compare MPMC vs
// Mutex pipelines without changing any business logic.
//
// The order book is confined to this thread — no locks needed on the book
// itself. Concurrency is handled entirely at the queue boundaries.
template <template <typename> class OQ = MPMCQueue,
          template <typename> class FQ = MPMCQueue>
class MatchingEngine {
public:
    MatchingEngine(OQ<Order>& order_rx,
                   FQ<Fill>&  fill_tx,
                   std::atomic<bool>& shutdown) noexcept
        : order_rx_(order_rx), fill_tx_(fill_tx), shutdown_(shutdown) {}

    // Runs until shutdown is set. Spins on order_rx; yields on empty queue
    // to avoid burning 100% CPU when the feed is idle.
    void run() {
        Order order;
        while (!shutdown_.load(std::memory_order_relaxed)) {
            if (!order_rx_.pop(order)) {
                std::this_thread::yield();
                continue;
            }

            const uint64_t fill_ns = now_ns();

            auto it = books_.find(order.symbol);
            if (it == books_.end()) {
                auto [ins, ok] = books_.try_emplace(order.symbol, order.symbol);
                it = ins;
            }

            const auto fills = it->second.match_order(order, fill_ns);
            for (const Fill& fill : fills) {
                // Spin-wait if the fill queue is full (reporter falling behind).
                // In production, this would trigger a backpressure signal upstream.
                while (!fill_tx_.push(fill)) {
                    if (shutdown_.load(std::memory_order_relaxed)) return;
                    std::this_thread::yield();
                }
            }
        }
    }

private:
    OQ<Order>&         order_rx_;
    FQ<Fill>&          fill_tx_;
    std::atomic<bool>& shutdown_;
    std::unordered_map<Symbol, OrderBook, SymbolHash> books_;
};

} // namespace fxmatch

#endif // FXMATCH_ENGINE_HPP
